using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using NUnit.Framework;
using SmartCampusSandbox.AzureFunctions;
using System;
using System.IO;
using Shouldly;

namespace Tests
{
    public class TransformTests
    {
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
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(jsonContent);
            IoTWorXOutput output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.TransformMsgToIotWorXOutput(
                DateTime.UtcNow, telemetryDataPoint);

            output.Gateway.ShouldBe("b19IoTWorx");
            output.FullTagName.ShouldBe("Device_190131_AV_90");
            output.DeviceName.ShouldBe("190131");
            output.Object.ShouldBe("AV");
            output.Instance.ShouldBe(90);
            output.Value.ShouldBe("73.000000");
            output.Unit.ShouldBe("DEGREES-FAHRENHEIT");
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-10T23:48:43.667Z").ToUniversalTime().ToString("o"));
            DateTime.Parse(output.EventEnqueuedUtcTime).ToUniversalTime().ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.DeviceStatus.ShouldBe(bool.TrueString);
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
            dynamic telemetryDataPoint = JsonConvert.DeserializeObject(jsonContent);
            IoTWorXOutput output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.TransformMsgToIotWorXOutput(
                DateTime.UtcNow, telemetryDataPoint);

            output.Gateway.ShouldBe("b19IoTWorx");
            output.FullTagName.ShouldBe("Device_190131_AI_10");
            output.DeviceName.ShouldBe("190131");
            output.Object.ShouldBe("AI");
            output.Instance.ShouldBe(10);
            output.Value.ShouldBe("121.000000");
            output.Unit.ShouldBe("CUBIC-FEET-PER-MINUTE");
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-12T19:46:52.174Z").ToUniversalTime().ToString("o"));
            DateTime.Parse(output.EventEnqueuedUtcTime).ToUniversalTime().ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
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
                }");
            IoTWorXOutput output = SmartCampusSandbox.AzureFunctions.IoTWorxBuildingDataProcessingFunction.TransformMsgToIotWorXOutput(
                DateTime.UtcNow, telemetryDataPoint);

            output.Gateway.ShouldBe("b19IoTWorx");
            output.FullTagName.ShouldBe("Device_190130_AV_67");
            output.DeviceName.ShouldBe("190130");
            output.Object.ShouldBe("AV");
            output.Instance.ShouldBe(67);
            output.Value.ShouldBe("400.000000");
            output.Unit.ShouldBeEmpty();
            output.DeviceTimestamp.ShouldBe(DateTime.Parse("2019-06-10T23:48:43.667Z").ToUniversalTime().ToString("o"));
            DateTime.Parse(output.EventEnqueuedUtcTime).ToUniversalTime().ShouldBe(DateTime.UtcNow, TimeSpan.FromMilliseconds(1000));
            output.DeviceStatus.ShouldBe(bool.TrueString);
        }
    }
}