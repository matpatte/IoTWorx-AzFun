using System;
using System.Collections;
using System.Collections.Generic;
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
        private static readonly Uri DocumentCollectionUri = UriFactory.CreateDocumentCollectionUri("smartcampussandbox", "Devices");

        [FunctionName("IoTWorxBuildingDataProcessingFunction")]
        public static async Task Run(
            // Incoming events delivered to the IoTHub trigger this Fn
            [IoTHubTrigger(
                eventHubName: "messages/events",
                Connection = "IoTHubTriggerConnection",
                ConsumerGroup = "dev")] EventData[] eventHubMessages,

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

            ILogger log,
            System.Threading.CancellationToken token)
        {
            /* Example inbound JSON from IoTWorx
            * {
            "gwy": "b19IoTWorx",
            "name": "Device_190131_AI_10",
            "value": "121.000000 CUBIC-FEET-PER-MINUTE",
            "timestamp": "2019-06-12T19:46:52.174Z",
            "status": true
            }
            */

            //Deserialize all the inbound messages in the array, preserving properties
            var messages = eventHubMessages
                .Select(data => new
                {
                    Body = (dynamic) JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data.Body)),
                    SystemProperties = data.SystemProperties,
                    Properties = data.Properties
                })
                .ToList();

            //Retrieve all the devices by Id in the inbound group
            var knownDeviceDocuments = await GetKnownDeviceData(messages, docDbClient, DocumentCollectionUri, log, token);
            List<string> unidentifiedDevices = new List<string>();

            foreach (var telemetryDataPoint in messages)
            {
                if (token.IsCancellationRequested)
                {
                    log.LogWarning("Function was cancelled.");
                    break;
                }
                 
                string logMsg = $"Telemetry Received : " +
                    "Name {telemetryDataPoint.name}" +
                    "value {telemetryDataPoint.value}" +
                    "timestamp {telemetryDataPoint.timestamp}" +
                    "status {telemetryDataPoint.status}";

                var deviceDoc = knownDeviceDocuments.SingleOrDefault(document => document.Id == telemetryDataPoint.Body.name);
                if (deviceDoc != null)
                {
                    //update the device doc with current value and time
                    deviceDoc = TransformMsgToDeviceDoc(
                        telemetryDataPoint.SystemProperties.EnqueuedTimeUtc, telemetryDataPoint.Body, deviceDoc);

                    //Write back to DocDb
                    await outputDeviceDocumentsUpdated.AddAsync(deviceDoc, token);
                    
                    //Send to EventHub
                    await outputEvents.AddAsync(JsonConvert.SerializeObject(deviceDoc), token);

                    log.LogDebug(JsonConvert.SerializeObject(deviceDoc, Formatting.Indented));
                }
                else
                {
                    //device id not found in database, 
                    //Todo - add to queue of unidentified devices
                    unidentifiedDevices.Add(telemetryDataPoint.Body.name);
                }
                log.LogError($"device document not found for the following ids: {String.Join(',', unidentifiedDevices)} ");

            }
        }

        private static async Task<List<DeviceDocument>> GetKnownDeviceData(
            IEnumerable<dynamic> messages,
            DocumentClient docDbClient,
            Uri createDocumentCollectionUri,
            ILogger log,
            CancellationToken token)
        {
            var knownDevices = new List<DeviceDocument>();

            var deviceIds = messages.Select(x => x.Body.name).ToList();

            if (!deviceIds.Any())
            {
                log.LogError("No device ids found.");
            }
            else
            {
                log.LogInformation($"searching for the following devicesIds: {String.Join(',', deviceIds)}");

                var query = docDbClient.CreateDocumentQuery<DeviceDocument>(createDocumentCollectionUri)
                    .Where(document => deviceIds.Contains(document.Id))
                    .AsDocumentQuery();
                
                while (query.HasMoreResults)
                {
                    knownDevices.AddRange(await query.ExecuteNextAsync<DeviceDocument>(token));
                }
            }

            return knownDevices;
        }

        public static DeviceDocument TransformMsgToDeviceDoc(DateTime enqueuedTimeUtc, dynamic telemetryDataPoint, DeviceDocument inputDeviceDocument)
        {            
            var parsedDeviceName = ((string)telemetryDataPoint.name).Split('_');

            string unparsedValue = telemetryDataPoint.value.ToString();

            (string value, string unit) = ParseValueUnit(unparsedValue, parsedDeviceName[2]);
            
            //Gateway = telemetryDataPoint.gwy,
            //FullTagName = telemetryDataPoint.name,
            inputDeviceDocument.DeviceName = parsedDeviceName[1];
            inputDeviceDocument.Object = parsedDeviceName[2]; //Object is a reserved word, hence the @
            inputDeviceDocument.Instance = int.Parse(parsedDeviceName[3] ?? "");
            inputDeviceDocument.PresentValue = value;
            inputDeviceDocument.Units = unit;
            inputDeviceDocument.DeviceTimestamp = telemetryDataPoint.timestamp.ToString("o");
            inputDeviceDocument.DeviceStatus = telemetryDataPoint.status;
            inputDeviceDocument.EventEnqueuedUtcTime = enqueuedTimeUtc;

            return inputDeviceDocument;
        }

        /// <summary>
        /// Parses the value and returns
        /// </summary>
        /// <param name="value"></param>
        /// <param name="objectType"></param>
        /// <returns></returns>
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

    public class DeviceDocument
    {
        public string Id { get; set; }
        public string Region { get; set; }
        public string Campus{ get; set; } 
        public string Building{ get; set; } 
        public string Floor{ get; set; } 
        public string Room{ get; set; } 
        public string Tag{ get; set; }
        public string DeviceName { get; set; }
        public string @Object { get; set; } //Object is a reserved word, hence the @
        public int Instance { get; set; }
        public string PresentValue{ get; set; } 
        public string Units{ get; set; } 
        public DateTimeOffset EventEnqueuedUtcTime{ get; set; }
        public string DeviceStatus { get; set; }
        public string DeviceTimestamp { get; set; }
    }
}
