using System;
using System.IO;
using Microsoft.BizTalk.Message.Interop;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Winterdom.BizTalk.PipelineTesting;
using BizTalkComponents.DbUtils;
using BizTalkComponents.Utils;
using BizTalkComponents.PipelineComponents.PromoteDbQueryResult;

namespace BizTalkComponents.PromoteDbQueryResult.Tests.UnitTests
{
    [TestClass]
    public class XPathValueExistsTests
    {
        private const int ValueExistsTrue = 1;
        private const int ValueExistsFalse = 0;

        // Test message
        private const string TestMessage = @"Test.xml";

        // Pipeline properties
        private string inputDbColumnType = "String";
        private string inputDbColumnName = "MyColumn";
        private string inputDbTableName = "MyTable";
        private string inputXPath = "/*[local-name()='order']/*[local-name()='storeid']";
        private ContextProperty outputContextProperty = new ContextProperty("OutputTestProperty", "http://test.namespace.com");

        [TestMethod]
        public void TestXPathValueExists_True()
        {
            // Mock the IDbQuery, return value exist
            var mock = new Mock<IDbQuery>();
            mock.Setup(t => t.ExecuteScalar(It.IsAny<String>())).Returns(ValueExistsTrue).Verifiable();

            var component = new XPathValueExists
            {
                DbQuery = mock.Object,
                DbColumnType = inputDbColumnType,
                DbColumnName = inputDbColumnName,
                DbTableName = inputDbTableName,
                InputXPath = inputXPath,
                OutputPropertyPath = outputContextProperty.ToPropertyString()
            };

            IBaseMessage message = ProcessMessage(component);

            // Correct property is promoted and existed in database
            Assert.AreEqual(true, message.Context.IsPromoted(outputContextProperty));
            Assert.AreEqual(true, (bool)message.Context.Read(outputContextProperty));

            // Mocked object called
            mock.Verify();
        }

        [TestMethod]
        public void TestXPathValueExists_False()
        {
            // Mock the IDbQuery, return value exist
            var mock = new Mock<IDbQuery>();
            mock.Setup(t => t.ExecuteScalar(It.IsAny<String>())).Returns(ValueExistsFalse).Verifiable();

            var component = new XPathValueExists
            {
                DbQuery = mock.Object,
                DbColumnType = inputDbColumnType,
                DbColumnName = inputDbColumnName,
                DbTableName = inputDbTableName,
                InputXPath = inputXPath,
                OutputPropertyPath = outputContextProperty.ToPropertyString()
            };

            IBaseMessage message = ProcessMessage(component);

            // Correct property is promoted and did not exist in database
            Assert.AreEqual(true, message.Context.IsPromoted(outputContextProperty));
            Assert.AreEqual(false, (bool)message.Context.Read(outputContextProperty));

            // Mocked object called
            mock.Verify();
        }

        private IBaseMessage ProcessMessage(XPathValueExists component)
        {
            using (var file = File.Open(TestMessage, FileMode.Open))
            {
                IBaseMessage input = MessageHelper.CreateFromStream(file);

                var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
                pipeline.AddComponent(component, PipelineStage.Decode);

                MessageCollection output = pipeline.Execute(input);
                return output[0];
            }
        }
    }
}
