using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using NUnit.Framework;
using SmartCampusSandbox.AzureFunctions;
using System;
using Shouldly;
using Microsoft.Azure.WebJobs;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;

namespace SmartCampusSandbox.Test
{
    [TestFixture]
    public class EventHandlingTests
    {

        [Test]
        public async Task FlagsUnprovisionedDevicesTest()
        {
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            var provisionedDeviceDocuments = new List<DeviceDocument>();

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            BACNetTelemetryMsg ioTWorxBacNetMsg = new BACNetTelemetryMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            BACNetIoTHubMessage iotBacNetEventHubMessage = new BACNetIoTHubMessage(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            var messages = new List<BACNetIoTHubMessage>()
            {
                { iotBacNetEventHubMessage}
            };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, provisionedDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and flagged accordingly
            outputDeviceDocs.Items.Count.ShouldBe(1);
            DeviceDocument deviceDocument = outputDeviceDocs.Items[0];
            deviceDocument.id.ShouldBe(ioTWorxBacNetMsg.name);
            deviceDocument.PresentValue.ShouldBe(ioTWorxBacNetMsg.value);
            deviceDocument.DeviceStatus.ShouldBe("Unprovisioned");

            //Don't send Unprovisioned device events downstream
            outputEvents.Items.ShouldBeEmpty();
        }

        [Test]
        public async Task UnprovisionedRemainsTest()
        {
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            BACNetTelemetryMsg ioTWorxBacNetMsg = new BACNetTelemetryMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "true",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            BACNetIoTHubMessage iotBacNetEventHubMessage = new BACNetIoTHubMessage(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            var messages = new List<BACNetIoTHubMessage>()
            {
                { iotBacNetEventHubMessage}
            };

            //Ensure this device is Provisioned as "Unprovisioned" and stays that way
            var provisionedDeviceDocuments = new List<DeviceDocument>() {
                { new DeviceDocument() { id = ioTWorxBacNetMsg.name, DeviceStatus = "Unprovisioned" } }
            };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, provisionedDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and remain in Unprovisioned state
            outputDeviceDocs.Items.Count.ShouldBe(1);
            DeviceDocument outDeviceDocument = outputDeviceDocs.Items[0];
            outDeviceDocument.id.ShouldBe(ioTWorxBacNetMsg.name);
            outDeviceDocument.PresentValue.ShouldBe(ioTWorxBacNetMsg.value);
            outDeviceDocument.DeviceStatus.ShouldBe("Unprovisioned");

            //Don't send Unprovisioned device events downstream
            outputEvents.Items.Count.ShouldBe(0);
        }



        [Test]
        public async Task BothProvisionedAndUnprovisionedDeviceDataSavedToCosmos()
        {
            //Create 2 messages, one for a provisioned and the other an unprovisioned device
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            BACNetTelemetryMsg ioTWorxBacNetMsg = new BACNetTelemetryMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "true",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            BACNetIoTHubMessage provisionedIoTBacNetEventHubMessage = new BACNetIoTHubMessage(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            //Clone Provisioned into Unprovisioned and modify the name
            var unprovisionedIoTBacNetEventHubMessage =
                JsonConvert.DeserializeObject<BACNetIoTHubMessage>(JsonConvert.SerializeObject(provisionedIoTBacNetEventHubMessage));
            unprovisionedIoTBacNetEventHubMessage.BACNetMsg.name = "unprovisioned_device_name";

            var messages = new List<BACNetIoTHubMessage>()
            {
                { provisionedIoTBacNetEventHubMessage},
                {unprovisionedIoTBacNetEventHubMessage}
            };

            //Ensure device 1 is "Provisioned" (and 2 isn't)
            var provisionedDeviceDocuments = new List<DeviceDocument>() {
                { new DeviceDocument() { id = provisionedIoTBacNetEventHubMessage.BACNetMsg.name, DeviceStatus = "Provisioned" } }
            };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, provisionedDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and flagged accordingly
            outputDeviceDocs.Items.Count.ShouldBe(2);

            DeviceDocument deviceDocument = outputDeviceDocs.Items[0];
            deviceDocument.id.ShouldBe(provisionedIoTBacNetEventHubMessage.BACNetMsg.name);
            deviceDocument.PresentValue.ShouldBe(provisionedIoTBacNetEventHubMessage.BACNetMsg.value);
            deviceDocument.DeviceStatus.ShouldBe("true");

            DeviceDocument unprovisionedDeviceDocument = outputDeviceDocs.Items[1];
            unprovisionedDeviceDocument.id.ShouldBe(unprovisionedIoTBacNetEventHubMessage.BACNetMsg.name);
            unprovisionedDeviceDocument.PresentValue.ShouldBe(unprovisionedIoTBacNetEventHubMessage.BACNetMsg.value);
            unprovisionedDeviceDocument.DeviceStatus.ShouldBe("Unprovisioned");
        }

        [Test]
        public async Task OnlyProvisionedDeviceDataSentToEventHub()
        {
            //Create 2 messages, one for a provisioned and the other an unprovisioned device
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            BACNetTelemetryMsg ioTWorxBacNetMsg = new BACNetTelemetryMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "true",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            BACNetIoTHubMessage provisionedIoTBacNetEventHubMessage = new BACNetIoTHubMessage(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            //Clone Provisioned into Unprovisioned and modify the name
            var unprovisionedIoTBacNetEventHubMessage =
                JsonConvert.DeserializeObject<BACNetIoTHubMessage>(JsonConvert.SerializeObject(provisionedIoTBacNetEventHubMessage));
            unprovisionedIoTBacNetEventHubMessage.BACNetMsg.name = "unprovisioned_device_name";

            var messages = new List<BACNetIoTHubMessage>()
            {
                { provisionedIoTBacNetEventHubMessage},
                {unprovisionedIoTBacNetEventHubMessage}
            };

            //Ensure device 1 is "Provisioned" (and 2 isn't)
            var provisionedDeviceDocuments = new List<DeviceDocument>() {
                { new DeviceDocument() { id = provisionedIoTBacNetEventHubMessage.BACNetMsg.name, DeviceStatus = "Provisioned" } }
            };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, provisionedDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and flagged accordingly
            outputDeviceDocs.Items.Count.ShouldBe(2);

            DeviceDocument deviceDocument = outputDeviceDocs.Items[0];
            deviceDocument.id.ShouldBe(provisionedIoTBacNetEventHubMessage.BACNetMsg.name);
            deviceDocument.PresentValue.ShouldBe(provisionedIoTBacNetEventHubMessage.BACNetMsg.value);
            deviceDocument.DeviceStatus.ShouldBe("true");

            DeviceDocument unprovisionedDeviceDocument = outputDeviceDocs.Items[1];
            unprovisionedDeviceDocument.id.ShouldBe(unprovisionedIoTBacNetEventHubMessage.BACNetMsg.name);
            unprovisionedDeviceDocument.PresentValue.ShouldBe(unprovisionedIoTBacNetEventHubMessage.BACNetMsg.value);
            unprovisionedDeviceDocument.DeviceStatus.ShouldBe("Unprovisioned");
        }



        [Test]
        public async Task LatestTelemetryWhenOutOfOrderInSameBatchForSameDeviceIsWritten()
        {
            //It's possible we could see a telemetry message older than one which has already been seen (in same or different batches)
            // in which case we should not replace present value, and not publish to the output event stream

            Assert.Inconclusive();  //ToDo - implement this test and the functionality for handling ordering
        }


        [Test]
        public async Task LatestTelemetryWhenOutOfOrderInDifferentBatchForSameDeviceIsWritten()
        {
            //It's possible we could see a telemetry message older than one which has already been seen (in same or different batches)
            // in which case we should not replace present value, and not publish to the output event stream

            Assert.Inconclusive();  //ToDo - implement this test and the functionality for handling ordering
        }
    }


    public class AsyncCollector<T> : IAsyncCollector<T>
    {
        public readonly List<T> Items = new List<T>();

        public Task AddAsync(T item, CancellationToken cancellationToken = default)
        {

            Items.Add(item);

            return Task.FromResult(true);
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }
    }
}

