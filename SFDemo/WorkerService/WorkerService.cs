using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using static System.Fabric.FabricClient;

namespace WorkerService
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class WorkerService : StatelessService
    {

        // The name of your queue
        const string QueueName = "Queuename";

        // QueueClient is thread-safe. Recommended that you cache 
        // rather than recreating it on every request

        ServiceBusClient client;

        // the processor that reads and processes messages from the queue
        ServiceBusProcessor processor;

        string ConnectionString;


        public WorkerService(StatelessServiceContext context)
            : base(context)
        {

            CodePackageActivationContext context1 = FabricRuntime.GetActivationContext();
            var configSettings = context1.GetConfigurationPackageObject("Config").Settings;
            ConnectionString = configSettings.Sections["MyConfigSection"].Parameters["ConnectionStr"].Value;
        }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

                long iterations = 0;

            try
            {


                var clientOptions = new ServiceBusClientOptions()
                {
                    TransportType = ServiceBusTransportType.AmqpWebSockets
                };

                client = new ServiceBusClient(ConnectionString, clientOptions);

                // create a processor that we can use to process the messages
                // TODO: Replace the <QUEUE-NAME> placeholder
                processor = client.CreateProcessor(QueueName, new ServiceBusProcessorOptions());


                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }


            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }

        async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            // complete the message. message is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }




        protected override Task OnOpenAsync(CancellationToken cancellationToken)
        {


            return base.OnOpenAsync(cancellationToken);
        }

        protected override void OnAbort()
        {
            base.OnAbort();

        }

        protected override Task OnCloseAsync(CancellationToken cancellationToken)
        {

            if (processor != null)
            {
                processor.StopProcessingAsync();
                processor.DisposeAsync();
            }

            if (client != null)
                client.DisposeAsync();

            return base.OnCloseAsync(cancellationToken);
        }
    }
}
