using CG.Business.Models;
using CG.Business.Repositories;
using CG.Linq.CosmoDb.Properties;
using CG.Linq.CosmoDb.Repositories.Options;
using CG.Validations;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using System;
using System.Data.Entity.Design.PluralizationServices;
using System.Globalization;
using System.Linq;

namespace CG.Linq.CosmoDb.Repositories
{
    /// <summary>
    /// This class is a CosmoDb implementation of the <see cref="ILinqRepository{TModel}"/>
    /// interface.
    /// </summary>
    /// <typeparam name="TOptions">The options type associated with the repository.</typeparam>
    /// <typeparam name="TModel">The model type associated with the repository.</typeparam>
    public class CosmoDbRepository<TOptions, TModel> : 
        LinqRepositoryBase<TOptions, TModel>,
        ILinqRepository<TModel>
        where TModel : class, IModel
        where TOptions : IOptions<CosmoDbRepositoryOptions>
    {
        // *******************************************************************
        // Properties.
        // *******************************************************************

        #region Properties

        /// <summary>
        /// This property contains a referenc to a CosmoDb client.
        /// </summary>
        protected CosmosClient Client { get; }

        /// <summary>
        /// This property contains a reference to a CosmoDb database.
        /// </summary>
        protected Lazy<Database> Database { get; }

        /// <summary>
        /// This property contains a reference to a CosmoDb container.
        /// </summary>
        protected Lazy<Container> Container { get; }

        /// <summary>
        /// This property contains the name of the CosboDb container.
        /// </summary>
        protected string ContainerName { get; set; }

        /// <summary>
        /// This property helps us track whether we've verified the underlying
        /// container exists, or not.
        /// </summary>
        protected static bool ContainerChecked { get; private set; }

        /// <summary>
        /// This property helps us track whether we've verified the underlying
        /// database exists, or not.
        /// </summary>
        protected static bool DatabaseChecked { get; private set; }

        #endregion

        // *******************************************************************
        // Constructors.
        // *******************************************************************

        #region Constructors

        /// <summary>
        /// This constructor creates a new instance of the <see cref="CosmoDbRepository{TOptions, TModel}"/>
        /// class.
        /// </summary>
        /// <param name="options">The options to use for the repository.</param>
        /// <param name="client">The CosmoDb client to use with the repository.</param>
        public CosmoDbRepository(
            TOptions options,
            CosmosClient client
            ) : base(options)
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(options, nameof(options))
                .ThrowIfNull(client, nameof(client));

            // Save the references.
            Client = client;

            // We'll tie the container back to the model type here by pluralizing
            //   the model's type name and using that as the container name.
            ContainerName = PluralizationService.CreateService(
                new CultureInfo("en") // Always pluralize in english
                ).Pluralize(typeof(TModel).Name);

            // Lazily create the database.
            Database = new Lazy<Database>(() =>
            {
                // Should we check the underyling database?
                if (false == DatabaseChecked)
                {
                    // If we get here then we need to ensure that the underlying database
                    //   actually exists, before we attempt to use it. We'll only do this
                    //   once though, because it's time consuming.

                    // Ensure the database exists.
                    var response = Client.CreateDatabaseIfNotExistsAsync(
                        Options.Value.DatabaseId
                        ).Result;

                    // Ensure the call worked.
                    if (System.Net.HttpStatusCode.OK != response.StatusCode)
                    {
                        // Panic!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CosmoDbCrudRepository_CreateDatabase,
                                response,
                                Options.Value.DatabaseId,
                                Options.Value.ConnectionString
                                )
                            );
                    }
                }

                // Remember we've checked the database.
                DatabaseChecked = true;

                // Get the database reference.
                var database = Client.GetDatabase(
                    Options.Value.DatabaseId
                    );

                // Return the database.
                return database;
            });

            // Lazily create the container.
            Container = new Lazy<Container>(() =>
            {
                // Create the properties for the container.
                var containerProperties = new ContainerProperties()
                {
                    Id = ContainerName,
                    PartitionKeyPath = "/key",
                    IndexingPolicy = new IndexingPolicy()
                    {
                        Automatic = false,
                        IndexingMode = IndexingMode.Lazy,
                    }
                };

                // Should we check the underyling container?
                if (false == ContainerChecked)
                {
                    // If we get here then we need to ensure that the underlying container
                    //   actually exists, before we attempt to use it. We'll only do this
                    //   once though, because it's time consuming.

                    // Ensure the container exists.
                    var response = Database.Value.CreateContainerIfNotExistsAsync(
                        containerProperties
                        ).Result;

                    // Ensure the call worked.
                    if (System.Net.HttpStatusCode.OK != response.StatusCode)
                    {
                        // Panic!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CosmoDbCrudRepository_CreateContainer,
                                ContainerName,
                                response,
                                Options.Value.DatabaseId,
                                Options.Value.ConnectionString
                                )
                            );
                    }
                }

                // Remember we've checked the container.
                ContainerChecked = true;

                // Get a reference to the container.
                var container = Client.GetContainer(
                    Options.Value.DatabaseId,
                    ContainerName
                    );

                // Return the container.
                return container;
            });
        }

        #endregion

        // *******************************************************************
        // Public methods.
        // *******************************************************************

        #region Public methods

        /// <inheritdoc />
        public override IQueryable<TModel> AsQueryable()
        {
            // Defer to the CosmoDb container.
            return Container.Value.GetItemLinqQueryable<CosmoDbWrapper<TModel>>(
                allowSynchronousQueryExecution: true
                ).Select(x => x.model)
                 .Cast<TModel>();
        }

        #endregion

        // *******************************************************************
        // Protected methods.
        // *******************************************************************

        #region Protected methods

        /// <summary>
        /// This method is called to clean up managed resources.
        /// </summary>
        /// <param name="disposing">True to cleanup managed resources.</param>
        protected override void Dispose(
            bool disposing
            )
        {
            // Should we cleanup managed resources?
            if (disposing)
            {
                Client?.Dispose();
            }

            // Give the base class a chance.
            base.Dispose(disposing);
        }

        #endregion
    }
}
