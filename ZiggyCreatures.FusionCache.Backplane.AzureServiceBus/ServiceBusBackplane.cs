using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane;

namespace ZiggyCreatures.FusionCache.Backplane.AzureServiceBus
{
	public class ServiceBusBackplane : IFusionCacheBackplane
	{
		private const String topicName = "fusionCache-dev";

		public ServiceBusBackplane(ServiceBusAdministrationClient serviceBusAdministrationClient, ServiceBusClient serviceBusClient)
		{
			this._serviceBusAdministrationClient = serviceBusAdministrationClient;
			this._serviceBusClient = serviceBusClient;
			this._serviceBusSender = new Lazy<ServiceBusSender>(() =>
			{
				return this._serviceBusClient.CreateSender(topicName);
			});
		}

		private readonly ServiceBusAdministrationClient _serviceBusAdministrationClient;
		private readonly ServiceBusClient _serviceBusClient;
		private readonly Lazy<ServiceBusSender> _serviceBusSender;
		private ServiceBusProcessor _serviceBusProcessor;

		public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
		{
			this.PublishAsync(message, options, token).GetAwaiter().GetResult();
		}

		public async ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
		{
			await this._serviceBusSender.Value.SendMessageAsync(new ServiceBusMessage() { Body = new BinaryData(BackplaneMessage.ToByteArray(message)) }, token);
		}

		public void Subscribe(BackplaneSubscriptionOptions options)
		{
			this.SubscribeAsync(options).GetAwaiter().GetResult();
		}

		public void Unsubscribe()
		{
			this.UnsubscribeAsync().GetAwaiter().GetResult();
		}

		public async ValueTask SubscribeAsync(BackplaneSubscriptionOptions options)
		{
			if (!await this._serviceBusAdministrationClient.TopicExistsAsync(topicName))
			{
				await this._serviceBusAdministrationClient.CreateTopicAsync(topicName);
			}
			var subcriptionName = $"fusion-{options.CacheInstanceId}";
			if (!await this._serviceBusAdministrationClient.SubscriptionExistsAsync(topicName, subcriptionName))
			{
				await this._serviceBusAdministrationClient.CreateSubscriptionAsync(topicName, subcriptionName);
			}
			this._serviceBusProcessor = this._serviceBusClient.CreateProcessor(topicName, subcriptionName);
			this._serviceBusProcessor.ProcessErrorAsync += async (args) =>
			{
			};
			this._serviceBusProcessor.ProcessMessageAsync += async (args) =>
			{
				var data = args.Message.Body.ToArray();
				var msg = BackplaneMessage.FromByteArray(data);
				if (options.IncomingMessageHandlerAsync != null)
				{
					await options.IncomingMessageHandlerAsync(msg);
				}
			};
			await this._serviceBusProcessor.StartProcessingAsync();
		}

		public async ValueTask UnsubscribeAsync()
		{
			await this._serviceBusProcessor.StopProcessingAsync();
			await this._serviceBusProcessor.DisposeAsync();
			// TODO delete subscription 
		}
	}
}
