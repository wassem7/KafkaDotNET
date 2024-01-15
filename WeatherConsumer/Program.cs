using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig()
{
    BootstrapServers = "localhost:9092",
    GroupId = "weather-consumers",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("weather-topic");

CancellationTokenSource token = new();

try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);

        if (response.Message != null)
        {
            var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
            Console.WriteLine($"State : {weather.State} - Temperature - {weather.Temperature}F");
        }
    }
}
catch (Exception e)
{
    Console.WriteLine(e);
}

public record Weather(string State, int Temperature);
