using Confluent.Kafka;
using System;

namespace Consumer_MicroService
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "sps",

                BootstrapServers = "localhost:9092",

                AutoOffsetReset=AutoOffsetReset.Latest,

                EnableAutoCommit=true,

            };
            using (var consumer = new ConsumerBuilder<Null,string>(config).Build())
            {
                
                consumer.Subscribe("demo");
                while(true)
                {
                    var cr = consumer.Consume();
                    Console.WriteLine(cr.Message.Value);
                }
            }

        }
    }
}
