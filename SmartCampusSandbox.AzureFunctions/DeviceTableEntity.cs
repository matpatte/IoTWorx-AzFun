using System;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace SmartCampusSandbox.AzureFunctions
{
    public class DeviceTableEntity :TableEntity
    {
        public BACNetIoTHubMessage BACNetIoTHubMsg {get; set;}

        public DeviceDocument DeviceDoc {get; set;}

        public DeviceTableEntity(DeviceDocument deviceDoc)
        {
            this.PartitionKey = deviceDoc.GatewayName;
            this.RowKey = deviceDoc.DeviceName;
            this.Timestamp = DateTime.UtcNow;
            this.DeviceDoc = deviceDoc;
            this.Text = JsonConvert.SerializeObject(deviceDoc);
        }

        public DeviceTableEntity(BACNetIoTHubMessage bacNetIoTHubMsg)
        {
            this.PartitionKey = bacNetIoTHubMsg.BACNetMsg.gwy;
            this.RowKey = bacNetIoTHubMsg.BACNetMsg.name;
            this.Timestamp = DateTime.UtcNow;
            this.BACNetIoTHubMsg = bacNetIoTHubMsg;
            this.Text = JsonConvert.SerializeObject(bacNetIoTHubMsg);
        }

        public string Text {get; set;}
    }
}