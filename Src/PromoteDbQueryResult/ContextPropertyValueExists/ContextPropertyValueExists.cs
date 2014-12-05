using System;
using System.ComponentModel;
using Microsoft.BizTalk.Message.Interop;
using Microsoft.BizTalk.Component.Interop;
using BizTalkComponents.Utils;
using BizTalkComponents.DbUtils;

namespace BizTalkComponents.PipelineComponents.PromoteDbQueryResult
{
    [ComponentCategory(CategoryTypes.CATID_PipelineComponent)]
    [System.Runtime.InteropServices.Guid("83b46613-fcbc-4d48-9e54-351acce56a47")]
    [ComponentCategory(CategoryTypes.CATID_Any)]
    public partial class ContextPropertyValueExists : Microsoft.BizTalk.Component.Interop.IComponent, IBaseComponent, IPersistPropertyBag, IComponentUI
    {
        private const string DbColumnNamePropertyName       = "DbColumnName";
        private const string DbColumnTypePropertyName       = "DbColumnType";
        private const string DbConnectionStringPropertyName = "DbConnectionString";
        private const string DbProviderPropertyName         = "DbProvider";
        private const string DbTableNamePropertyName        = "DbTableName";
        private const string InputPropertyPathPropertyName  = "InputPropertyPath";
        private const string OutputPropertyPathPropertyName = "OutputPropertyPath";

        //private string _DbFilter;

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
        public string DbProvider         { get; set; }

        [Description("The name of the database table")]
        [DisplayName("Database table name")]
        public string DbTableName        { get; set; }

        [Description("The input context property")]
        [DisplayName("Input property path")]
        public string InputPropertyPath  { get; set; }

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
            
            string identifier = GetIdentifier(inmsg.Context);
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

            if (string.IsNullOrEmpty(InputPropertyPath))
            {
                InputPropertyPath = PropertyBagHelper.ToStringOrDefault(PropertyBagHelper.ReadPropertyBag(propertyBag, InputPropertyPathPropertyName), string.Empty);
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
            PropertyBagHelper.WritePropertyBag(propertyBag, InputPropertyPathPropertyName, InputPropertyPath);
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

        private string GetIdentifier(IBaseMessageContext context)
        {
            object propertyValue;

            context.TryRead(new ContextProperty(InputPropertyPath), out propertyValue);
   
            return (string)propertyValue;
        }

    }

}
