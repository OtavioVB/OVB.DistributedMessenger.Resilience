
using Confluent.Kafka;
using WebApi.Consumers;

namespace WebApi;

public sealed class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Logging.AddConsole();

        builder.Services.AddControllers();
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        builder.Services.AddSingleton(builder.Configuration.GetRequiredSection("ConsumerConfig").Get<ConsumerConfig>()!);
        builder.Services.AddSingleton(builder.Configuration.GetRequiredSection("ProducerConfig").Get<ProducerConfig>()!);

        builder.Services.AddHostedService<CustomerCreatedConsumer>();

        var app = builder.Build();

        app.UseSwagger();
        app.UseSwaggerUI();
        app.MapControllers();
        app.Run();
    }
}
