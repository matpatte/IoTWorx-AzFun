using System;

namespace SmartCampusSandbox.AzureFunctions
{
    public class DeviceDocument
    {
        public string id { get; set; }
        public string GatewayName {get; set;}
        public string DeviceName { get; set; }
        public string Region { get; set; }
        public string Campus { get; set; }
        public string Building { get; set; }
        public string Floor { get; set; }
        public string Unit { get; set; }
        public string ObjectType { get; set; } //Object is a reserved word, hence the @
        public int Instance { get; set; }

        public string EquipmentClass { get; set; }
        public string EquipmentModel { get; set; }
        public string SubsystemClass { get; set; }
        public string SubsystemModel { get; set; }
        public string TagName { get; set; }
        public string FullAssetPath { get; set; }
        public string Equipment { get; set; }

        public string PresentValue{ get; set; }
        public string ValueUnits{ get; set; }
        public DateTimeOffset EventEnqueuedUtcTime{ get; set; }
        public string DeviceStatus { get; set; }
        public DateTimeOffset DeviceTimestamp { get; set; }
    }
}
