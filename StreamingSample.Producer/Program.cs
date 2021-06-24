using System;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using MassTransit;
using MassTransit.KafkaIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;
using Tweetinvi;
using Tweetinvi.Models;

namespace StreamingSample.Producer
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
                    rider.AddProducer<KafkaMessage>("random-tweets");
            
                    rider.UsingKafka((context, k) =>
                    {
                        k.Host("localhost:9093");
                    });
                });
            });
            
            var provider = services.BuildServiceProvider();
            var busControl = provider.GetRequiredService<IBusControl>();
            await busControl.StartAsync();

            var client = new TwitterClient();
            var stream = client.Streams.CreateTweetStream();
            stream.EventReceived += async (_, eventReceived) =>
            {
                // Console.WriteLine(eventReceived.Json);
                var producer = provider.GetRequiredService<ITopicProducer<KafkaMessage>>();
                await producer.Produce(new { client.Json.Deserialize<ITweet>(eventReceived.Json).Text });
            };
            await stream.StartAsync("https://stream.twitter.com/1.1/statuses/sample.json");
        }
    }
    
    public interface KafkaMessage
    {
        string Text { get; }
    }
}
