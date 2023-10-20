using System;
using System.Data.SqlClient;
using Confluent.Kafka;

namespace Kafka
{
    internal class Program
    {
        private static string connectionString = @"YourConnectionString";
        private static SqlConnection connection;
        private static SqlCommand command;
        private static string query;

        public Program()
        {
            connection = new SqlConnection(connectionString);
            connection.Open();
        }

        static void Main(string[] args)
        {
            Program program = new Program();

                var anotherConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(anotherConfig).Build())
            {
                bool a = false;
                string b = "";

                query = "SELECT * FROM InfoDb";
                using (command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            a = Convert.ToBoolean(reader["TestOrLive"]);
                            b = reader["ConnectionString"].ToString();
                        }
                    }

                }
                string messagess = b;
                var message = new Message<Null, string> { Value = messagess };
                producer.Produce("test-topic", message);
                producer.Flush(TimeSpan.FromSeconds(10)); // Tüm mesajları teslim etmek için bekler

            }

            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var config2 = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            using (var consumer = new ConsumerBuilder<Ignore, string>(config2).Build())
            {
                consumer.Subscribe("test-topic");

                while (true)
                {
                    var message = consumer.Consume();
                    string getMessage = message.Message.Value;

                    InsertIntoDatabase(getMessage);
                }
            }
        }

        private static void InsertIntoDatabase(string message)
        {
            query = "INSERT INTO ReadedData (ConnectionString) VALUES (@ConnectionString)";
            using (command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@ConnectionString", message);
                command.ExecuteNonQuery();
            }
        }
    }
}