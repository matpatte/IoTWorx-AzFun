using System.Collections.Generic;
using Microsoft.Azure.EventHubs;

namespace SmartCampusSandbox.AzureFunctions
{
    public class BACNetIoTHubMessage
    {
        public BACNetTelemetryMsg BACNetMsg { get; }
        public EventData.SystemPropertiesCollection SystemProperties { get; }
        public IDictionary<string, object> Properties { get; }

        public BACNetIoTHubMessage(BACNetTelemetryMsg bacNetMsg,
            EventData.SystemPropertiesCollection systemProperties,
            IDictionary<string, object> properties)
        {
            BACNetMsg = bacNetMsg;
            SystemProperties = systemProperties;
            Properties = properties;
        }
    }
}
