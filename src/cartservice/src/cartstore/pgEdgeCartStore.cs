// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Linq;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using Grpc.Core;
using Npgsql;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;

namespace cartservice.cartstore
{
    public class pgEdgeCartStore  : ICartStore
    {
        private readonly string tableName;
        private readonly string connectionString;

        public pgEdgeCartStore(IConfiguration configuration)
        {
            string databaseName = configuration["POSTGRES_DATABASE_NAME"];
            string pgEdgeHostList = configuration["PGEDGE_HOST_LIST"];
            string username = configuration["POSTGRES_USERNAME"];
            string password = configuration["POSTGRES_PASSWORD"];

            string selectedHost = SelectHostWithLowestLatency(pgEdgeHostList, username, password, databaseName);
            connectionString = $"Host={selectedHost};Username={username};Password={password};Database={databaseName}";
            tableName = configuration["POSTGRES_TABLE_NAME"];
        }

        private string SelectHostWithLowestLatency(string hostList, string username, string password, string databaseName)
        {
            var hosts = hostList.Split(',').Select(host => host.Trim()).ToList();
            double lowestAverageLatency = double.MaxValue;
            string selectedHost = string.Empty;

            foreach (var host in hosts)
            {
                var hostParts = host.Split(':');
                var hostName = hostParts[0];
                var port = int.Parse(hostParts.Length > 1 ? hostParts[1] : "5432"); // Default PostgreSQL port is 5432

                var latencies = new List<double>();
                for (int i = 0; i < 5; i++)
                {
                    try
                    {
                        var stopwatch = new Stopwatch();

                        // Connection string for Npgsql
                        var connectionString = $"Host={host};Username={username};Password={password};Database={databaseName}";

                        using (var conn = new NpgsqlConnection(connectionString))
                        {
                            stopwatch.Start();
                            
                            conn.Open();
                            using (var cmd = new NpgsqlCommand("SELECT VERSION();", conn))
                            {
                                cmd.ExecuteScalar(); // Execute the query
                            }

                            stopwatch.Stop();
                        }

                        double latency = stopwatch.Elapsed.TotalMilliseconds;
                        latencies.Add(latency);
                    }
                    catch
                    {
                        // Ignore any exceptions and move to the next attempt
                    }

                    Thread.Sleep(1000); // Wait before the next attempt
                }

                // Calculate average latency for the current host
                if (latencies.Count > 0)
                {
                    double averageLatency = latencies.Average();

                    Console.WriteLine($"latency for pgEdge server: {host}, is: {averageLatency:0.00}ms");

                    if (averageLatency < lowestAverageLatency)
                    {
                        lowestAverageLatency = averageLatency;
                        selectedHost = host;
                    }
                }
            }

            Console.WriteLine($"Selected pgEdge server: {selectedHost}, Latency: {lowestAverageLatency:0.00}ms");

            return selectedHost;
        }

        public async Task AddItemAsync(string userId, string productId, int quantity)
        {
            Console.WriteLine($"AddItemAsync for {userId} called");
            try
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync();

                // Fetch the current quantity for our userId/productId tuple
                var fetchCmd = $"SELECT quantity FROM {tableName} WHERE userID='{userId}' AND productID='{productId}'";
                var currentQuantity = 0;
                await using (var cmdRead = new NpgsqlCommand(fetchCmd, conn))
                await using (var reader = await cmdRead.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                        currentQuantity += reader.GetInt32(0);
                }
                var totalQuantity = quantity + currentQuantity;

                var insertCmd = $"INSERT INTO {tableName} (userId, productId, quantity) VALUES ('{userId}', '{productId}', {totalQuantity}) ON CONFLICT (userId, productId) DO UPDATE SET quantity = EXCLUDED.quantity;";
                await using (var cmdInsert = new NpgsqlCommand(insertCmd, conn))
                {
                    await cmdInsert.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                throw new RpcException(
                    new Status(StatusCode.FailedPrecondition, $"Can't access cart storage at {connectionString}. {ex}"));
            }
        }

        public async Task<Hipstershop.Cart> GetCartAsync(string userId)
        {
            Console.WriteLine($"GetCartAsync called for userId={userId}");
            Hipstershop.Cart cart = new();
            cart.UserId = userId;
            try
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync();

                var cartFetchCmd = $"SELECT productId, quantity FROM {tableName} WHERE userId = '{userId}'";
                await using (var cmd = new NpgsqlCommand(cartFetchCmd, conn))
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        Hipstershop.CartItem item = new()
                        {
                            ProductId = reader.GetString(0),
                            Quantity = reader.GetInt32(1)
                        };
                        cart.Items.Add(item);
                    }
                }
                return cart;
            }
            catch (Exception ex)
            {
                throw new RpcException(
                    new Status(StatusCode.FailedPrecondition, $"Can't access cart storage at {connectionString}. {ex}"));
            }
        }

        public async Task EmptyCartAsync(string userId)
        {
            Console.WriteLine($"EmptyCartAsync called for userId={userId}");

            try
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync();

                var deleteCmd = $"DELETE FROM {tableName} WHERE userID = '{userId}'";
                await using (var cmd = new NpgsqlCommand(deleteCmd, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                throw new RpcException(
                    new Status(StatusCode.FailedPrecondition, $"Can't access cart storage at {connectionString}. {ex}"));
            }
        }

        public bool Ping()
        {
            try
            {
                using var conn = new NpgsqlConnection(connectionString);
                conn.Open();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}