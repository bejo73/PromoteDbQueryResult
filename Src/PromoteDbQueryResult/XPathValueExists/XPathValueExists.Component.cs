using System;
using System.Collections;

namespace BizTalkComponents.PipelineComponents.PromoteDbQueryResult
{
    public partial class XPathValueExists
    {
        public string Name { get { return "PromoteDbQueryResult.XPathValueExists"; } }
        public string Version { get { return "1.0"; } }
        public string Description { get { return "Promotes 'True' to a specified context property if a specified value exists in database, 'False' otherwise."; } }

        public void GetClassID(out Guid classID)
        {
            classID = new System.Guid("83b46613-1973-4d48-9e54-351acce56a47");
        }

        public void InitNew()
        {

        }

        public IEnumerator Validate(object projectSystem)
        {
            return null;
        }

        public IntPtr Icon { get { return IntPtr.Zero; } }
    }
}
