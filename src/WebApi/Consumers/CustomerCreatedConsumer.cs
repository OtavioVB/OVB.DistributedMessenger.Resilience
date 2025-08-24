
using Confluent.Kafka;
using System.Text.Json;
using WebApi.Domain;

namespace WebApi.Consumers;

public sealed class CustomerCreatedConsumer : BackgroundService
{
    private readonly ILogger<CustomerCreatedConsumer> _logger;
    private readonly IServiceProvider _serviceProvider;

    public CustomerCreatedConsumer(
        ILogger<CustomerCreatedConsumer> logger,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    private const string TOPIC_NAME = "corporate.distributed-messenger.customer.created";
    private const string TOPIC_NAME_RETRY = "corporate.distributed-messenger.customer.created-retry";
    private const string TOPIC_NAME_DLQ = "corporate.distributed-messenger.customer.created-dlq";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = _serviceProvider.GetRequiredService<ConsumerConfig>();

        var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(TOPIC_NAME); 

        _ = Task.Run(async () =>
        {
            _logger.LogInformation("[{Type}][{Method}] The apache kafka consumer has been started. Info = {@Info}",
                nameof(CustomerCreatedConsumer),
                nameof(ExecuteAsync),
                new
                {
                    EventType = nameof(CustomerCreatedConsumer),
                    GroupId = consumerConfig.GroupId,
                    TopicName = TOPIC_NAME,
                });

            while (!stoppingToken.IsCancellationRequested) 
            {
                var @event = consumer.Consume(stoppingToken);
                
                await ExecuteHandleAsync(@event, stoppingToken);
            }

            consumer.Close();
            consumer.Dispose();
        }, stoppingToken);

        var consumerDlq = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumerDlq.Subscribe(TOPIC_NAME_DLQ);

        _ = Task.Run(async () =>
        {
            _logger.LogInformation("[{Type}][{Method}] The apache kafka dlq consumer has been started. Info = {@Info}",
                nameof(CustomerCreatedConsumer),
                nameof(ExecuteAsync),
                new
                {
                    EventType = nameof(CustomerCreatedConsumer),
                    GroupId = consumerConfig.GroupId,
                    TopicName = TOPIC_NAME,
                });

            while (!stoppingToken.IsCancellationRequested)
            {
                var @event = consumerDlq.Consume(stoppingToken);

                await ExecuteHandleAsync(@event, stoppingToken);
            }

            consumerDlq.Close();
            consumerDlq.Dispose();
        });

        return Task.CompletedTask;
    }

    public async Task ExecuteHandlingToDlqAsync(ConsumeResult<string, string> @event, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteHandleAsync(@event, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                exception: ex,
                "[{Type}][{Method}] Message has been received and sent to DLQ queue. Message = {@Message}",
                nameof(CustomerCreatedConsumer),
                nameof(ExecuteHandlingToDlqAsync),
                JsonSerializer.Serialize(@event));

            var producerConfig = _serviceProvider.GetRequiredService<ProducerConfig>();

            var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            var message = new Message<string, string>()
            {
                Key = @event.Message.Key, 
                Value = @event.Message.Value
            };

            await producer.ProduceAsync(
                topic: TOPIC_NAME_DLQ,
                message: message,
                cancellationToken: cancellationToken);
        }
    }

    public async Task ExecuteHandleAsync(ConsumeResult<string, string> @event, CancellationToken cancellationToken = default)
    {
        var message = JsonSerializer.Deserialize<Customer>(@event.Message.Value);

        throw new Exception("error");

        _logger.LogInformation("[{Type}][{Method}] Message has been received and processed. Message = {@Message}",
            nameof(CustomerCreatedConsumer),
            nameof(ExecuteHandleAsync),
            JsonSerializer.Serialize(@event));

        await Task.Delay(100, cancellationToken);
    }
}
