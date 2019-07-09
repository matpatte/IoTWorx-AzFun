using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

namespace SmartCampusSandbox.AzureFunctions
{
    public static class IoTWorxBuildingDataProcessingFunction
    {
        private static readonly Uri DocumentCollectionUri = UriFactory.CreateDocumentCollectionUri("SmartCampusSandbox", "Devices");
        private static readonly FeedOptions DocDbQueryOption = new FeedOptions { EnableCrossPartitionQuery = true };
        public const string DEVICE_STATUS_UNPROVISIONED = "Unprovisioned";

        [FunctionName("IoTWorxBuildingDataProcessingFunction")]
        public static async Task Run(
            // Incoming events delivered to the IoTHub trigger this Fn
            [IoTHubTrigger(
                eventHubName: "messages/events",
                Connection = "IoTHubTriggerConnection",
                ConsumerGroup = "%IoTHub_ConsumerGroup%")] EventData[] eventHubMessages,

            [CosmosDB(
                databaseName: "SmartCampusSandbox",
                collectionName: "Devices",
                ConnectionStringSetting = "CosmosDBConnection")] DocumentClient docDbClient,

            [CosmosDB(
                databaseName: "SmartCampusSandbox",
                collectionName: "Devices",
                ConnectionStringSetting = "CosmosDBConnection")] IAsyncCollector<DeviceDocument> outputDeviceDocumentsUpdated,

            // Outgoing transformed event data is delivered to this Event Hub
            [EventHub(
                eventHubName: "iotworxoutputevents",
                Connection = "EventHubConnectionAppSetting")] IAsyncCollector<string> outputEvents,

            // Incomming messages which aren't found in the reference data set will be sent here instead of to EH
            [Table("UnprovisionedDeviceTelemetry",
                Connection ="AzureWebJobsStorage")] IAsyncCollector<DeviceTableEntity> outputUnprovisionedDevices,

            ILogger log,
            System.Threading.CancellationToken token)
        {
            //Todo - determine what the inbound BACNet message format is and desrialized/transformed accordingly

            //Deserialize all the inbound messages in the batch, preserving properties
            var messages = eventHubMessages
                .Select(data => new BACNetIoTHubMessage(
                    JsonConvert.DeserializeObject<BACNetTelemetryMsg>(Encoding.UTF8.GetString(data.Body)),
                    data.SystemProperties,
                    data.Properties))
                .ToList();

            //Retrieve all the devices by Id in the inbound group
            var knownDeviceDocuments = await GetKnownDeviceData(messages, docDbClient, DocumentCollectionUri, log, token);

            // Handle all the messages in the batch
            await HandleMessageBatch(messages, knownDeviceDocuments, outputDeviceDocumentsUpdated, outputEvents, outputUnprovisionedDevices, log, token);

        }

        public static async Task HandleMessageBatch(
            List<BACNetIoTHubMessage> messageBatch,
            List<DeviceDocument> knownDeviceDocuments,
            IAsyncCollector<DeviceDocument> deviceDocumentsWriteCollector,
            IAsyncCollector<string> eventCollector,
            IAsyncCollector<DeviceTableEntity> unprovisionedDeviceCollector,
            ILogger log,
            CancellationToken cancellationToken)
        {
            List<string> unprovisioned = new List<string>();

            foreach (var bacNetIoTHubMsg in messageBatch)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    log.LogWarning("Function was cancelled.");
                    break;
                }

                DeviceDocument deviceDoc = GetKnownOrNewDeviceDoc(knownDeviceDocuments, bacNetIoTHubMsg.BACNetMsg.name);

                //update the device doc with inbound telemetry (value, timestamp, status, etc.)
                deviceDoc = ApplyTelemetryToDeviceDoc(bacNetIoTHubMsg, deviceDoc);

                //UpSERT the device document to Cosmos
                await deviceDocumentsWriteCollector.AddAsync(deviceDoc, cancellationToken);

                if (String.Equals(deviceDoc.DeviceStatus, DEVICE_STATUS_UNPROVISIONED, StringComparison.OrdinalIgnoreCase))
                {   //Handle Unprovisioned Devices here
                    unprovisioned.Add(bacNetIoTHubMsg.BACNetMsg.name);
                    await unprovisionedDeviceCollector.AddAsync(new DeviceTableEntity(bacNetIoTHubMsg),cancellationToken);
                }
                else
                {
                    //Provisioned Device only code
                    await eventCollector.AddAsync(JsonConvert.SerializeObject(deviceDoc), cancellationToken);
                }
            }

            if (unprovisioned.Any())
                log.LogError($"Unprovisioned DeviceIds encountered: {String.Join(',', unprovisioned)} ");
        }

        private static DeviceDocument GetKnownOrNewDeviceDoc(
            List<DeviceDocument> knownDeviceDocuments,
            string deviceId)
        {
            //Lookup Known Devices in CosmosDB by Id
            var deviceDoc = knownDeviceDocuments.SingleOrDefault(
                document => document.id == deviceId);

            if (deviceDoc == null) //not found, so create a new one and mark it unprovisioned
            {
                deviceDoc = new DeviceDocument()
                {
                    id = deviceId,
                    DeviceStatus = DEVICE_STATUS_UNPROVISIONED
                };
            }

            return deviceDoc;
        }

        private static async Task<List<DeviceDocument>> GetKnownDeviceData(
            IEnumerable<BACNetIoTHubMessage> messages,
            DocumentClient docDbClient,
            Uri createDocumentCollectionUri,
            ILogger log,
            CancellationToken token)
        {
            var knownDevices = new List<DeviceDocument>();

            var deviceIds = messages.Select(x => x.BACNetMsg.name).ToList();

            if (!deviceIds.Any())
            {
                log.LogError("No device ids found.");
            }
            else
            {
                log.LogDebug($"searching for the following devicesIds: {String.Join(',', deviceIds)}");

                var query = docDbClient.CreateDocumentQuery<DeviceDocument>(createDocumentCollectionUri, DocDbQueryOption)
                    .Where(document => deviceIds.Contains(document.id))
                    .AsDocumentQuery();

                while (query.HasMoreResults)
                {
                    knownDevices.AddRange(await query.ExecuteNextAsync<DeviceDocument>(token));
                }
            }

            return knownDevices;
        }

        public static DeviceDocument ApplyTelemetryToDeviceDoc(
            BACNetIoTHubMessage bacNetIoTHubMessage,
            DeviceDocument inputDeviceDocument)
        {
            //Todo figure out how this function becomes modularized/configurable
            var parsedDeviceName = ((string)bacNetIoTHubMessage.BACNetMsg.name).Split('_');

            string unparsedValue = bacNetIoTHubMessage.BACNetMsg.value.ToString();

            (string value, string unit) = ParseValueUnit(unparsedValue, parsedDeviceName[2]);

            inputDeviceDocument.PresentValue = value;
            inputDeviceDocument.ValueUnits = unit;
            inputDeviceDocument.DeviceTimestamp = DateTimeOffset.Parse((string)bacNetIoTHubMessage.BACNetMsg.timestamp, styles: DateTimeStyles.RoundtripKind);

            //preserve Unprovisioned status
            if (!String.Equals(inputDeviceDocument.DeviceStatus, DEVICE_STATUS_UNPROVISIONED, StringComparison.OrdinalIgnoreCase)){
                inputDeviceDocument.DeviceStatus = bacNetIoTHubMessage.BACNetMsg.status;
            }
            inputDeviceDocument.EventEnqueuedUtcTime = bacNetIoTHubMessage.SystemProperties?.EnqueuedTimeUtc ?? DateTime.UtcNow;

            return inputDeviceDocument;
        }

        public static (string value, string unit) ParseValueUnit(
            string value,
            string objectType)
        {
            string trimedVal = value.Trim();
            string outputValue;
            string outputUnits = string.Empty;

            switch (objectType)
            {
                case "AI":
                case "AV":
                {
                    if (trimedVal.Contains(' '))
                    {
                        var splitValUnit = trimedVal.Split(' ');
                        outputValue = splitValUnit[0];
                        outputUnits = splitValUnit[1];

                    }
                    else
                    {
                        outputValue = trimedVal;
                    }
                    break;
                }
                case "BV":
                    outputValue = trimedVal;
                    break;
                default:
                    outputValue = trimedVal;
                    break;
            }
            return (outputValue, outputUnits);
        }


    }
}
