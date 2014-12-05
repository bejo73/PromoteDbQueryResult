using System;
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
    public class ContextPropertyValueExistsTests
    {
        private const int ValueExistsTrue = 1;
        private const int ValueExistsFalse = 0;

        private ContextProperty inputContextProperty = new ContextProperty("InputTestProperty", "http://test.namespace.com");
        private ContextProperty outputContextProperty = new ContextProperty("OutputTestProperty", "http://test.namespace.com");

        private const string TestMessage = "<test/>";
        private const string InputContextPropertyValue = "test";

        [TestMethod]
        public void TestContextPropertyValueExists_True()
        {
            // Mock the IDbQuery, return value exist
            var mock = new Mock<IDbQuery>();
            mock.Setup(t => t.ExecuteScalar(It.IsAny<String>())).Returns(ValueExistsTrue).Verifiable();

            var component = new ContextPropertyValueExists
            {
                DbQuery = mock.Object,
                DbColumnType = "Int",
                InputPropertyPath = inputContextProperty.ToPropertyString(),
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
        public void TestContextPropertyValueExists_False()
        {
            // Mock the IDbQuery, return value exist
            var mock = new Mock<IDbQuery>();
            mock.Setup(t => t.ExecuteScalar(It.IsAny<String>())).Returns(ValueExistsFalse).Verifiable();

            var component = new ContextPropertyValueExists
            {
                DbQuery = mock.Object,
                DbColumnType = "Int",
                InputPropertyPath = inputContextProperty.ToPropertyString(),
                OutputPropertyPath = outputContextProperty.ToPropertyString()
            };

            IBaseMessage message = ProcessMessage(component);

            // Correct property is promoted and did not exist in database
            Assert.AreEqual(true, message.Context.IsPromoted(outputContextProperty));
            Assert.AreEqual(false, (bool)message.Context.Read(outputContextProperty));

            // Mocked object called
            mock.Verify();
        }

        private IBaseMessage ProcessMessage(ContextPropertyValueExists component)
        {
            IBaseMessage input = MessageHelper.CreateFromString(TestMessage);

            input.Context.Promote(inputContextProperty, InputContextPropertyValue);

            var pipeline = PipelineFactory.CreateEmptyReceivePipeline();
            pipeline.AddComponent(component, PipelineStage.Decode);

            MessageCollection output = pipeline.Execute(input);
            return output[0];
        }
    }
}
