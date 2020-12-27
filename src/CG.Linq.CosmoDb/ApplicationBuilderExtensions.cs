using CG.Linq.CosmoDb.Properties;
using CG.Linq.CosmoDb.Repositories.Options;
using CG.Validations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;

namespace CG.Linq.CosmoDb
{
    /// <summary>
    /// This delegate type represents a callback to seed a database.
    /// </summary>
    /// <typeparam name="TClient">The type of associated CosmoDb client.</typeparam>
    /// <param name="client">The CosmoDb client to use for the operation.</param>
    /// <param name="wasDropped">Indicates whether the data-context was recently dropped.</param>
    /// <param name="wasCreated">Indicates whether the data-context was recently created.</param>
    /// <param name="wasMigrated">Indicates whether the data-context was recently migrated.</param>
    public delegate void SeedAction<in TClient>(
        TClient client,
        bool wasDropped,
        bool wasCreated
        ) where TClient : CosmosClient;



    /// <summary>
    /// This class contains extension methods related to the <see cref="IApplicationBuilder"/>
    /// type.
    /// </summary>
    public static partial class ApplicationBuilderExtensions
    {
        // *******************************************************************
        // Public methods.
        // *******************************************************************

        #region Public methods

        /// <summary>
        /// This method performs any startup logic required by CosmoDb, such as 
        /// dropping the underlying database (if needed), or creating the underlying 
        /// database (if needed), or adding seed data to an otherwise blank 
        /// database. 
        /// </summary>
        /// <typeparam name="TClient">The type of assciated client.</typeparam>
        /// <typeparam name="TOptions">The type of associated options.</typeparam>
        /// <param name="applicationBuilder">The application builder to use for 
        /// the operation.</param>
        /// <param name="hostEnvironment">The hosting environment to use for the
        /// operation.</param>
        /// <param name="seedDelegate">A delegate for seeding the database with 
        /// startup data.</param>
        /// <returns>The value of the <paramref name="applicationBuilder"/>
        /// parameter, for chaining calls together.</returns>
        /// <exception cref="ArgumentException">This exception is thrown whenever one
        /// or more arguments are invalid, or missing.</exception>
        public static IApplicationBuilder UseCosmoDb<TClient, TOptions>(
            this IApplicationBuilder applicationBuilder,
            IWebHostEnvironment hostEnvironment,
            SeedAction<TClient> seedDelegate
            ) where TClient : CosmosClient
              where TOptions : CosmoDbRepositoryOptions
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(applicationBuilder, nameof(applicationBuilder))
                .ThrowIfNull(hostEnvironment, nameof(hostEnvironment))
                .ThrowIfNull(seedDelegate, nameof(seedDelegate));

            // Get the registered options.
            var options = applicationBuilder.ApplicationServices.GetRequiredService<
                IOptions<TOptions>
                >();

            var wasDropped = false;
            var wasCreated = false;

            // Should we manipulate the database?
            if (options.Value.EnsureCreated ||
                options.Value.DropDatabase ||
                options.Value.SeedDatabase)
            {
                // Apply any pending migrations.
                using (var scope = applicationBuilder.ApplicationServices.CreateScope())
                {
                    // Get a CosmoDb client.
                    var client = scope.ServiceProvider.GetService<TClient>();

                    // Only perform data seeding on a developers machine.
                    if (hostEnvironment.EnvironmentName == "Development")
                    {
                        // Should we drop the database? 
                        if (options.Value.DropDatabase)
                        {
                            // Get a database reference.
                            var database = client.GetDatabase(
                                options.Value.DatabaseId
                                );

                            // Drop the database.
                            database.DeleteAsync();

                            // Keep track of what we've done.
                            wasDropped = true;
                        }

                        // Should we make sure the database exists?
                        if (options.Value.EnsureCreated)
                        {
                            // Create the database (if needed).
                            var response = client.CreateDatabaseIfNotExistsAsync(
                                options.Value.DatabaseId
                                ).Result;

                            // Did the call fail?
                            if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                                System.Net.HttpStatusCode.Created != response.StatusCode &&
                                System.Net.HttpStatusCode.Accepted != response.StatusCode)
                            {
                                // Panic!
                                throw new InvalidOperationException(
                                    message: string.Format(
                                        Resources.CreateDatabase,
                                        options.Value.DatabaseId,
                                        response.StatusCode,
                                        options.Value.ConnectionString
                                        )
                                    );
                            }

                            // Keep track of what we've done.
                            wasCreated = true;
                        }
                    }

                    // Should we make sure the database has seed data?
                    if (options.Value.SeedDatabase)
                    {
                        // Only perform data seeding on a developers machine.
                        if (hostEnvironment.EnvironmentName == "Development")
                        {
                            // Perform the data seeding operation.
                            seedDelegate(
                                client,
                                wasDropped,
                                wasCreated
                                );
                        }
                    }
                }
            }

            // Return the application builder.
            return applicationBuilder;
        }

        #endregion
    }
}
