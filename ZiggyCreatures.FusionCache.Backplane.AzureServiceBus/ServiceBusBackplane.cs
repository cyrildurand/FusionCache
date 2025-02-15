using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

namespace ZiggyCreatures.Caching.Fusion.Backplane.AzureServiceBus
{
	public class ServiceBusBackplane : IFusionCacheBackplane, IAsyncDisposable
	{
		private static String GenerateId()
		{
			const String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
			Random random = new Random();
			return new String(Enumerable.Range(0, 8)
				.Select(_ => chars[random.Next(chars.Length)])
				.ToArray());
		}

		public ServiceBusBackplane(
			ServiceBusAdministrationClient serviceBusAdministrationClient,
			ServiceBusClient serviceBusClient,
			String topicName,
			ILogger<ServiceBusBackplane> logger)
		{
			this._instanceId = GenerateId();
			this._serviceBusAdministrationClient = serviceBusAdministrationClient;
			this._serviceBusClient = serviceBusClient;
			this._topicName = topicName;
			this._logger = logger;
		}

		private readonly Object _lock = new();
		private readonly String _instanceId;
		private readonly ServiceBusAdministrationClient _serviceBusAdministrationClient;
		private readonly ServiceBusClient _serviceBusClient;
		private readonly String _topicName;
		private readonly ILogger _logger;


		private Boolean _isSubscribed = false;

		private String? _cacheName;
		private String? _channelName;
		private String? _cacheInstanceId;
		private String? _subscriptionName;
		private Func<BackplaneMessage?, Task>? _incomingMessageHandler;

		private ServiceBusProcessor? _serviceBusProcessor;
		private ServiceBusSender? _serviceBusSender;

		#region Sync methods interface

		public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
		{
			this.PublishAsync(message, options, token).GetAwaiter().GetResult();
		}
		public void Subscribe(BackplaneSubscriptionOptions options)
		{
			this.SubscribeAsync(options).GetAwaiter().GetResult();
		}
		public void Unsubscribe()
		{
			this.UnsubscribeAsync().GetAwaiter().GetResult();
		}

		#endregion


		public async ValueTask SubscribeAsync(BackplaneSubscriptionOptions options)
		{
			if (options is null)
			{
				throw new ArgumentNullException(nameof(options));
			}
			if (options.ChannelName is null)
			{
				throw new NullReferenceException($"The {nameof(BackplaneSubscriptionOptions)}.{nameof(options.ChannelName)} cannot be null");
			}
			if (options.CacheName is null)
			{
				throw new NullReferenceException($"The {nameof(BackplaneSubscriptionOptions)}.{nameof(options.CacheName)} cannot be null");
			}
			if (options.CacheInstanceId is null)
			{
				throw new NullReferenceException($"The {nameof(BackplaneSubscriptionOptions)}.{nameof(options.CacheInstanceId)} cannot be null");
			}
			if (options.IncomingMessageHandler == null && options.IncomingMessageHandlerAsync == null)
			{
				throw new ArgumentException("At least one of the incoming message handlers must be provided.");
			}

			this._cacheName = options.CacheName;
			this._channelName = options.ChannelName;
			this._cacheInstanceId = options.CacheInstanceId;
			this._subscriptionName = this.GenerateSubscriptionName(options);
			this._incomingMessageHandler = async (msg) =>
			{
				if (msg == null)
				{
					return;
				}
				if (options.IncomingMessageHandlerAsync != null)
				{
					await options.IncomingMessageHandlerAsync(msg);
				}
				if (options.IncomingMessageHandler != null)
				{
					options.IncomingMessageHandler(msg);
				}
			};

			await this.EnsureTopic();
			await this.EnsureSubscription();

			var serviceBusProcessor = this.EnsureProcessor();
			await serviceBusProcessor.StartProcessingAsync();

			if (options.ConnectHandler != null)
			{
				options.ConnectHandler(new BackplaneConnectionInfo(false));
			}
			if (options.ConnectHandlerAsync != null)
			{
				await options.ConnectHandlerAsync(new BackplaneConnectionInfo(false));
			}
		}

		public async ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
		{
			var sender = this.EnsureSender();

			await sender.SendMessageAsync(new ServiceBusMessage()
			{
				Body = new BinaryData(BackplaneMessage.ToByteArray(message)),
				Subject = this._cacheName,
				TimeToLive = options.Duration
			}, token);
		}


		public async ValueTask UnsubscribeAsync()
		{
			if (this._serviceBusProcessor != null)
			{
				await this._serviceBusProcessor.StopProcessingAsync();
			}
			await this._serviceBusAdministrationClient.DeleteSubscriptionAsync(this._topicName, this._subscriptionName);
			this._isSubscribed = false;
		}

		public async ValueTask DisposeAsync()
		{
			if (this._isSubscribed)
			{
				try
				{
					await this.UnsubscribeAsync();
				}
				catch (Exception ex)
				{
					this._logger.LogWarning(ex, "FUSION [N={CacheName} I={CacheInstanceId} SN={subscriptionName}]: [BP] an error occurred while unsubscribing", this._cacheName, this._cacheInstanceId, this._subscriptionName);
				}
			}

			if (this._serviceBusProcessor != null)
			{
				await this._serviceBusProcessor.DisposeAsync();
			}
		}


		private String GenerateSubscriptionName(BackplaneSubscriptionOptions options)
		{
			var subscriptionName = $"{this._instanceId}-{this._channelName}-{this._cacheName}";
			subscriptionName = Regex.Replace(subscriptionName, @"[^a-zA-Z0-9._-]", "_");

			if (subscriptionName.Length > 50)
			{
				subscriptionName = subscriptionName.Substring(0, 50);
				if (this._logger?.IsEnabled(LogLevel.Warning) ?? false)
				{
					this._logger.LogWarning("FUSION [N={CacheName} I={CacheInstanceId}]: [BP] the subscription name is too long and has been truncated to {SubscriptionName}", this._cacheName, this._cacheInstanceId, subscriptionName);
				}
			}
			return subscriptionName;
		}

		private async ValueTask EnsureTopic()
		{
			if (!await this._serviceBusAdministrationClient.TopicExistsAsync(this._topicName))
			{
				await this._serviceBusAdministrationClient.CreateTopicAsync(this._topicName);
			}
		}
		private async ValueTask EnsureSubscription()
		{
			if (!await this._serviceBusAdministrationClient.SubscriptionExistsAsync(this._topicName, this._subscriptionName))
			{
				await this._serviceBusAdministrationClient.CreateSubscriptionAsync(new CreateSubscriptionOptions(this._topicName, this._subscriptionName)
				{
					AutoDeleteOnIdle = TimeSpan.FromMinutes(5)
				});
			}
			else
			{
				if (this._logger?.IsEnabled(LogLevel.Information) ?? false)
				{
					this._logger.LogInformation("FUSION [N={CacheName} I={CacheInstanceId} SN={subscriptionName}]: [BP] the subscription already exists", this._cacheName, this._cacheInstanceId, this._subscriptionName);
				}
			}

			this._isSubscribed = true;
		}
		private ServiceBusProcessor EnsureProcessor()
		{
			if (this._serviceBusProcessor == null)
			{
				if (!Monitor.TryEnter(this._lock, TimeSpan.FromSeconds(5)))
				{
					throw new Exception("Unable to acquire lock after 5 seconds");
				}
				try
				{
					if (this._serviceBusProcessor == null)
					{
						this._serviceBusProcessor = this._serviceBusClient.CreateProcessor(this._topicName, this._subscriptionName, new ServiceBusProcessorOptions()
						{
							AutoCompleteMessages = true,
							Identifier = this._instanceId
						});

						this._serviceBusProcessor.ProcessErrorAsync += this.ProcessErrorAsync;
						this._serviceBusProcessor.ProcessMessageAsync += this.ProcessMessageAsync;
					}
				}
				finally
				{
					Monitor.Exit(this._lock);
				}
			}
			return this._serviceBusProcessor;
		}
		private ServiceBusSender EnsureSender()
		{
			if (this._serviceBusSender == null)
			{
				this.EnsureProcessor();

				if (!Monitor.TryEnter(this._lock, TimeSpan.FromSeconds(5)))
				{
					throw new Exception("Unable to acquire lock after 5 seconds");
				}
				try
				{
					if (this._serviceBusSender == null)
					{
						this._serviceBusSender = this._serviceBusClient.CreateSender(this._topicName);
					}
				}
				finally
				{
					Monitor.Exit(this._lock);
				}
			}
			return this._serviceBusSender;
		}

		private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
		{
			if (args.Message.Subject != this._cacheName)
			{
				return;
			}

			var data = args.Message.Body.ToArray();
			var msg = BackplaneMessage.FromByteArray(data);

			await this._incomingMessageHandler(msg);
		}
		private Task ProcessErrorAsync(ProcessErrorEventArgs args)
		{
			if (this._logger?.IsEnabled(LogLevel.Warning) ?? false)
			{
				this._logger.Log(LogLevel.Warning, args.Exception, "FUSION [N={CacheName} I={CacheInstanceId}]: [BP] an error occurred while processing a ServiceBus message", this._cacheName, this._cacheInstanceId);
			}

			return Task.CompletedTask;
		}

	}
}
