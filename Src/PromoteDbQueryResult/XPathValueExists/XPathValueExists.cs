using System;
using System.IO;
using System.ComponentModel;
using System.Xml;
using System.Xml.XPath;
using Microsoft.BizTalk.Message.Interop;
using Microsoft.BizTalk.Component.Interop;
using Microsoft.BizTalk.Streaming;
using Microsoft.BizTalk.XPath;
using BizTalkComponents.Utils;
using BizTalkComponents.DbUtils;

namespace BizTalkComponents.PipelineComponents.PromoteDbQueryResult
{
    [ComponentCategory(CategoryTypes.CATID_PipelineComponent)]
    [System.Runtime.InteropServices.Guid("83b46613-1973-4d48-9e54-351acce56a47")]
    [ComponentCategory(CategoryTypes.CATID_Any)]
    public partial class XPathValueExists : Microsoft.BizTalk.Component.Interop.IComponent, IBaseComponent, IPersistPropertyBag, IComponentUI
    {
        private const string DbColumnNamePropertyName       = "DbColumnName";
        private const string DbColumnTypePropertyName       = "DbColumnType";
        private const string DbConnectionStringPropertyName = "DbConnectionString";
        private const string DbProviderPropertyName         = "DbProvider";
        private const string DbTableNamePropertyName        = "DbTableName";
        private const string InputXPathPropertyName         = "InputXPath";
        private const string OutputPropertyPathPropertyName = "OutputPropertyPath";

        [Description("The name of the database column to check the input context property value against")]
        [DisplayName("Database column name")]
        public string DbColumnName { get; set; }

        [Description("The type of the database column, Int/String")]
        [DisplayName("Database column type")]
        public string DbColumnType { get; set; }

        [Description("The database connection string")]
        [DisplayName("Database connection string")]
        public string DbConnectionString { get; set; }

        [Description("The database provider")]
        [DisplayName("Database provider")]
        public string DbProvider { get; set; }

        [Description("The name of the database table")]
        [DisplayName("Database table name")]
        public string DbTableName { get; set; }

        [Description("The input XPath")]
        [DisplayName("Input XPath")]
        public string InputXPath { get; set; }

        [Description("The output context property path holding the result from the database query")]
        [DisplayName("Output property path")]
        public string OutputPropertyPath { get; set; }

        [Browsable(false)]
        public IDbQuery DbQuery { get; set; }

        public IBaseMessage Execute(IPipelineContext pc, IBaseMessage inmsg)
        {
            if (DbQuery == null)
            {
                DbQuery = new DbQuery(DbProvider, DbConnectionString);
            }

            string identifier = GetIdentifier(pc, inmsg);
            string query = CreateQueryString(identifier);
            int dbResult = DbQuery.ExecuteScalar(query);

            bool result = (dbResult > 0) ? true : false;
            inmsg.Context.Promote(new ContextProperty(OutputPropertyPath), result);

            return inmsg;
        }

        public void Load(IPropertyBag propertyBag, int errorLog)
        {
            if (string.IsNullOrEmpty(DbColumnName))
            {
                DbColumnName = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, DbColumnNamePropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(DbColumnType))
            {
                DbColumnType = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, DbColumnTypePropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(DbConnectionString))
            {
                DbConnectionString = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, DbConnectionStringPropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(DbProvider))
            {
                DbProvider = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, DbProviderPropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(DbTableName))
            {
                DbTableName = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, DbTableNamePropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(InputXPath))
            {
                InputXPath = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, InputXPathPropertyName), string.Empty);
            }

            if (string.IsNullOrEmpty(OutputPropertyPath))
            {
                OutputPropertyPath = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, OutputPropertyPathPropertyName), string.Empty);
            }
        }

        public void Save(IPropertyBag propertyBag, bool clearDirty, bool saveAllProperties)
        {
            PropertyBagHelper.WritePropertyBag(propertyBag, DbColumnNamePropertyName, DbColumnName);
            PropertyBagHelper.WritePropertyBag(propertyBag, DbColumnTypePropertyName, DbColumnType);
            PropertyBagHelper.WritePropertyBag(propertyBag, DbConnectionStringPropertyName, DbConnectionString);
            PropertyBagHelper.WritePropertyBag(propertyBag, DbProviderPropertyName, DbProvider);
            PropertyBagHelper.WritePropertyBag(propertyBag, DbTableNamePropertyName, DbTableName);
            PropertyBagHelper.WritePropertyBag(propertyBag, InputXPathPropertyName, InputXPath);
            PropertyBagHelper.WritePropertyBag(propertyBag, OutputPropertyPathPropertyName, OutputPropertyPath);
        }

        private string CreateQueryString(string identifier)
        {
            string quote = "'";
            if (DbColumnType.ToLower().Equals("int"))
            {
                quote = "";
            }

            return String.Format("SELECT COUNT(*) FROM {0} WHERE {1} = {2}{3}{4}", DbTableName, DbColumnName, quote, identifier, quote);
        }

        private string GetIdentifier(IPipelineContext pc, IBaseMessage inmsg)
        {
            string identifier = null;

            int bufferSize = 0x280;
            int thresholdSize = 0x100000;

            try
            {
                // Message body
                IBaseMessagePart bodyPart = inmsg.BodyPart;

                // Get stream
                Stream inboundStream = bodyPart.GetOriginalDataStream();
                VirtualStream virtualStream = new VirtualStream(bufferSize, thresholdSize);
                ReadOnlySeekableStream readOnlySeekableStream = new ReadOnlySeekableStream(inboundStream, virtualStream, bufferSize);

                // Reader
                XmlTextReader xmlTextReader = new XmlTextReader(readOnlySeekableStream);

                // xPath
                XPathCollection xPathCollection = new XPathCollection();
                XPathReader xPathReader = new XPathReader(xmlTextReader, xPathCollection);
                xPathCollection.Add(InputXPath);

                // Search for element
                bool elementFound = false;
                while (xPathReader.ReadUntilMatch())
                {
                    if (xPathReader.Match(0) && !elementFound)
                    {
                        elementFound = true;
                        identifier = xPathReader.ReadString();
                    }
                }

                // Reset
                readOnlySeekableStream.Position = 0;
                bodyPart.Data = readOnlySeekableStream;

                pc.ResourceTracker.AddResource(xmlTextReader);
            }
            catch (Exception ex)
            {
                if (inmsg != null)
                {
                    inmsg.SetErrorInfo(ex);
                }

                throw ex;
            }

            return identifier;
        }
    }
}