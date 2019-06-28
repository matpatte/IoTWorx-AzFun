using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using NUnit.Framework;
using SmartCampusSandbox.AzureFunctions;
using System;
using Shouldly;

namespace Tests
{
    public class TransformTests
    {
        private JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings(){DateParseHandling = DateParseHandling.None};

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void TransformTestWithUnit()
        {
            string jsonContent =
                @"{
                    'gwy': 'b19IoTWorx',
                    'name': 'Device_190131_AV_90',
                    'value': '73.000000 DEGREES-FAHRENHEIT',
                    'timestamp': '2019-06-10T23:48:43.667Z',
                    'status': true
                }";
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(jsonContent, _jsonSerializerSettings);
            DeviceDocument inputDeviceDocument = new DeviceDocument()
            {
                id = telemetryDataPoint.name,
                DeviceName = "190131",
                ObjectType = "AnalogValue",
                Instance = 90

            };
            DeviceDocument output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.ApplyTelemetryToDeviceDoc(
                DateTime.UtcNow, telemetryDataPoint, inputDeviceDocument);

            //output.Gateway.ShouldBe("b19IoTWorx");
            output.id.ShouldBe((string)telemetryDataPoint.name);
            output.EventEnqueuedUtcTime.ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.DeviceName.ShouldBe("190131");
            output.ObjectType.ShouldBe("AnalogValue");
            output.Instance.ShouldBe(90);
            output.PresentValue.ShouldBe("73.000000");
            output.ValueUnits.ShouldBe("DEGREES-FAHRENHEIT");
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-10T23:48:43.667Z"));
            output.DeviceStatus.ShouldBe(bool.TrueString);
            Console.Write(JsonConvert.SerializeObject(output));
        }


        [Test]
        public void TransformTestWithAIObject()
        {
            string jsonContent =
                @"{
                      'gwy': 'b19IoTWorx',
                      'name': 'Device_190131_AI_10',
                      'value': '121.000000 CUBIC-FEET-PER-MINUTE',
                      'timestamp': '2019-06-12T19:46:52.174Z',
                      'status': true
                  }";
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(jsonContent, _jsonSerializerSettings);
            DeviceDocument inputDeviceDocument = new DeviceDocument() { id = telemetryDataPoint.name };
            DeviceDocument output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.ApplyTelemetryToDeviceDoc(
                DateTime.UtcNow, telemetryDataPoint, inputDeviceDocument);

            output.EventEnqueuedUtcTime.ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.PresentValue.ShouldBe("121.000000");
            output.ValueUnits.ShouldBe("CUBIC-FEET-PER-MINUTE");
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-12T19:46:52.174Z"));
            output.DeviceStatus.ShouldBe(bool.TrueString);
        }

        [Test]
        public void TransformTestWithoutUnit()
        {
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(
                @"{
                  'gwy': 'b19IoTWorx',
                  'name': 'Device_190130_AV_67',
                  'value': '400.000000',
                  'timestamp': '2019-06-10T23:48:43.667Z',
                  'status': true
                }", _jsonSerializerSettings);
            DeviceDocument inputDeviceDocument = new DeviceDocument() { id = telemetryDataPoint.name };
            DeviceDocument output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.ApplyTelemetryToDeviceDoc(
                DateTime.UtcNow, telemetryDataPoint, inputDeviceDocument);

            output.EventEnqueuedUtcTime.ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.PresentValue.ShouldBe("400.000000");
            output.ValueUnits.ShouldBeEmpty();
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-10T23:48:43.667Z"));
            output.DeviceStatus.ShouldBe(bool.TrueString);
        }
        [Test]
        public void TransformTestWithBinaryValue()
        {
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(
                @"{
                      'gwy': 'b19IoTWorx',
                      'name': 'Device_190130_BV_66',
                      'value': 0,
                      'timestamp': '2019-06-10T23:48:43.667Z',
                      'status': true
                    }", _jsonSerializerSettings);
            DeviceDocument inputDeviceDocument = new DeviceDocument() { id = telemetryDataPoint.name };
            DeviceDocument output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.ApplyTelemetryToDeviceDoc(
                DateTime.UtcNow, telemetryDataPoint, inputDeviceDocument);

            output.EventEnqueuedUtcTime.ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.PresentValue.ShouldBe("0");
            output.ValueUnits.ShouldBeEmpty();
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-10T23:48:43.667Z"));
            output.DeviceStatus.ShouldBe(bool.TrueString);
        }
    }
}