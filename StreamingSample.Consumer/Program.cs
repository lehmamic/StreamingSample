using System;
using System.Threading.Tasks;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace StreamingSample.Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();
            
            var services = new ServiceCollection();
            services.TryAddSingleton<ILoggerFactory>(new SerilogLoggerFactory());
            services.TryAddSingleton(typeof(ILogger<>), typeof(Logger<>));

            services.AddMassTransit(x =>
            {
                // a rider does not work without a regular transportation
                x.UsingInMemory((context,cfg) => cfg.ConfigureEndpoints(context));

                x.AddRider(rider =>
                {
                    rider.AddConsumer<KafkaMessageConsumer>();

                    rider.UsingKafka((context, k) =>
                    {
                        k.Host("localhost:9093");

                        k.TopicEndpoint<KafkaMessage>("random-tweets", "consumer-group", e =>
                        {
                            e.ConfigureConsumer<KafkaMessageConsumer>(context);
                            e.CreateIfMissing(t =>
                            {
                                t.NumPartitions = 1; //number of partitions
                                t.ReplicationFactor = 1; //number of replicas
                            });
                        });
                    });
                });
            });
            
            var provider = services.BuildServiceProvider();
            var busControl = provider.GetRequiredService<IBusControl>();
            await busControl.StartAsync();

            while (true);
        }
    }
    
    class KafkaMessageConsumer : IConsumer<KafkaMessage>
    {
        public Task Consume(ConsumeContext<KafkaMessage> context)
        {
            Console.WriteLine(context.Message.Text);
            return Task.CompletedTask;
        }
    }

    public interface KafkaMessage
    {
        string Text { get; }
    }
}