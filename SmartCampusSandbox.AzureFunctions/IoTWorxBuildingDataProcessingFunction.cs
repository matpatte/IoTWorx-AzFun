using System;
using System.Text;
using System.Threading.Tasks;
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
        [FunctionName("IoTWorxBuildingDataProcessingFunction")]
        //Data is recorded in the following azure storage table
        //[return: Table("IoTWorXOutputTable")]
        public static async Task Run(
            // Incoming events delivered to the IoTHub trigger this Fn
            [IoTHubTrigger(
                eventHubName: "messages/events", 
                Connection = "IoTHubTriggerConnection", 
                ConsumerGroup = "smartcampussandbox")] EventData[] eventHubMessages,
            // Outgoing transformed event data is delivered to this Event Hub
            [EventHub(
                eventHubName: "iotworxoutputevents", 
                Connection = "EventHubConnectionAppSetting")] IAsyncCollector<string> outputEvents,
            ILogger log,
            System.Threading.CancellationToken token)
        {
            foreach (var message in eventHubMessages)
            {
                if (token.IsCancellationRequested)
                {
                    log.LogWarning("Function was cancelled.");
                    break;
                }

                string msg = Encoding.UTF8.GetString(message.Body);
                log.LogInformation("C# IoT Hub trigger function processed a message: {msg}", msg);

                /* Example inbound JSON from IoTWorx
                * {
                "gwy": "b19IoTWorx",
                "name": "Device_190131_AI_10",
                "value": "121.000000 CUBIC-FEET-PER-MINUTE",
                "timestamp": "2019-06-12T19:46:52.174Z",
                "status": true
                }
                */

                dynamic telemetryDataPoint = Newtonsoft.Json.JsonConvert.DeserializeObject(msg);

                string logMsg = $"Telemetry Received : " +
                    "Name {telemetryDataPoint.name}" +
                    "value {telemetryDataPoint.value}" +
                    "timestamp {telemetryDataPoint.timestamp}" +
                    "status {telemetryDataPoint.status}";

                IoTWorXOutput iconicsOutput = TransformMsgToIotWorXOutput(
                    message.SystemProperties.EnqueuedTimeUtc, telemetryDataPoint);

                log.LogInformation(JsonConvert.SerializeObject(iconicsOutput, Formatting.Indented));

                //Send to EventHub
                await outputEvents.AddAsync(JsonConvert.SerializeObject(iconicsOutput));

            }
        }

        public static IoTWorXOutput TransformMsgToIotWorXOutput(DateTime enqueuedTimeUtc, dynamic telemetryDataPoint)
        {            
            var parsedDeviceName = ((string)telemetryDataPoint.name).Split('_');

            string enqueuedTimeUtcString = enqueuedTimeUtc.ToString("o");

            string unparsedValue = telemetryDataPoint.value.ToString();

            (string value, string unit) = ParseValueUnit(unparsedValue, parsedDeviceName[2]);

            var iconicsOutput = new IoTWorXOutput
            {
                Gateway = telemetryDataPoint.gwy,
                FullTagName = telemetryDataPoint.name,
                DeviceName = parsedDeviceName[1],
                Object = parsedDeviceName[2], //Object is a reserved word, hence the @
                Instance = int.Parse(parsedDeviceName[3] ?? ""),
                Value = value,
                Unit = unit,
                DeviceTimestamp = telemetryDataPoint.timestamp.ToString("o"),
                DeviceStatus = telemetryDataPoint.status,
                EventEnqueuedUtcTime = enqueuedTimeUtcString
            };

            return iconicsOutput;
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
    public class IoTWorXOutput
    {
        public string Gateway { get; set; }
        public string FullTagName { get; set; }
        public string DeviceName { get; set; }
        public string @Object { get; set; } //Object is a reserved word, hence the @
        public string Value { get; set; }
        public string Unit { get; set; }
        public int Instance { get; set; }
        public string DeviceTimestamp { get; set; }
        public string DeviceStatus { get; set; }
        public string EventEnqueuedUtcTime { get; set; }

    }
}