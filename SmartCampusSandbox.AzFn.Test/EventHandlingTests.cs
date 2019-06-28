using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using NUnit.Framework;
using SmartCampusSandbox.AzureFunctions;
using System;
using Shouldly;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;

namespace SmartCampusSandbox.AzFn.Test
{
    [TestFixture]
    public class EventHandlingTests
    {

        [Test]
        public async Task FlagsUnprovisionedDevicesTest()
        {
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            var knownDeviceDocuments = new List<DeviceDocument>();

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            IoTWorxBACNetMsg ioTWorxBacNetMsg = new IoTWorxBACNetMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            IotBacNetEventHubMessageBatch iotBacNetEventHubMessage = new IotBacNetEventHubMessageBatch(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            var messages = new List<IotBacNetEventHubMessageBatch>()
            {
                { iotBacNetEventHubMessage}
            };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, knownDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and flagged accordingly
            outputDeviceDocs.Items.Count.ShouldBe(1);
            DeviceDocument deviceDocument = outputDeviceDocs.Items[0];
            deviceDocument.id.ShouldBe(ioTWorxBacNetMsg.name);
            deviceDocument.PresentValue.ShouldBe(ioTWorxBacNetMsg.value);
            deviceDocument.Unprovisioned.ShouldBe(true);

            //Don't send Unprovisioned device events downstream
            outputEvents.Items.ShouldBeEmpty();
        }

        [Test]
        public async Task PreviouslyUnprovisionedUnFlagsTest()
        {
            AsyncCollector<DeviceDocument> outputDeviceDocs = new AsyncCollector<DeviceDocument>();
            EventData ed = new EventData(new byte[0]);

            AsyncCollector<string> outputEvents = new AsyncCollector<string>();
            IoTWorxBACNetMsg ioTWorxBacNetMsg = new IoTWorxBACNetMsg()
            {
                name = "Device_190130_AV_67",
                value = "180",
                status = "",
                timestamp = DateTime.UtcNow.ToString("o")
            };
            IotBacNetEventHubMessageBatch iotBacNetEventHubMessage = new IotBacNetEventHubMessageBatch(
                ioTWorxBacNetMsg,
                ed.SystemProperties, new Dictionary<string, object>());

            var messages = new List<IotBacNetEventHubMessageBatch>()
            {
                { iotBacNetEventHubMessage}
            };

            //Ensure this device is "Known"
            var knownDeviceDocuments = new List<DeviceDocument>() { { new DeviceDocument() { id = ioTWorxBacNetMsg.name, Unprovisioned = false } } };

            await IoTWorxBuildingDataProcessingFunction.HandleMessageBatch(
                messages, knownDeviceDocuments, outputDeviceDocs, outputEvents,
                LoggerUtils.Logger<object>(), new CancellationToken());

            //Unprovisioned devices should be written to the DocDb and flagged accordingly
            outputDeviceDocs.Items.Count.ShouldBe(1);
            DeviceDocument deviceDocument = outputDeviceDocs.Items[0];
            deviceDocument.id.ShouldBe(ioTWorxBacNetMsg.name);
            deviceDocument.PresentValue.ShouldBe(ioTWorxBacNetMsg.value);
            deviceDocument.Unprovisioned.ShouldBe(false);

            //Don't send Unprovisioned device events downstream
            outputEvents.Items.Count.ShouldBe(1);
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

