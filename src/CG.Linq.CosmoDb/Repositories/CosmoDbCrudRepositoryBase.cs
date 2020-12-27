using CG.Business.Models;
using CG.Business.Repositories;
using CG.Linq.CosmoDb.Properties;
using CG.Linq.CosmoDb.Repositories;
using CG.Linq.CosmoDb.Repositories.Options;
using CG.Validations;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using System;
using System.Data.Entity.Design.PluralizationServices;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace CG.Linq.CosmoDb.Repositories
{
    /// <summary>
    /// This class is a base CosmoDb implementation of the <see cref="ICrudRepository{TModel, TKey}"/>
    /// interface.
    /// </summary>
    /// <typeparam name="TOptions">The options type associated with the repository.</typeparam>
    /// <typeparam name="TModel">The type of associated model.</typeparam>
    /// <typeparam name="TKey">The key type associated with the model.</typeparam>
    public abstract class CosmoDbCrudRepositoryBase<TOptions, TModel, TKey> :
        CrudRepositoryBase<TOptions, TModel, TKey>,
        ICrudRepository<TModel, TKey>
        where TModel : class, IModel<TKey>
        where TOptions : IOptions<CosmoDbRepositoryOptions>
        where TKey : new()
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
        /// This constructor creates a new instance of the <see cref="CosmoDbCrudRepositoryBase{TOptions, TModel, TKey}"/>
        /// class.
        /// </summary>
        /// <param name="options">The options to use for the repository.</param>
        /// <param name="client">The CosmoDb client to use with the repository.</param>
        protected CosmoDbCrudRepositoryBase(
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
                    // Ensure the database exists.
                    var response = Client.CreateDatabaseIfNotExistsAsync(
                        Options.Value.DatabaseId
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode)
                    {
                        // Panik!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CreateDatabase,
                                Options.Value.DatabaseId,
                                response.StatusCode,
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
                    PartitionKeyPath = "/id",
                    IndexingPolicy = new IndexingPolicy()
                    {
                        Automatic = false,
                        IndexingMode = IndexingMode.Lazy,
                    }
                };

                // Should we check the underyling container?
                if (false == ContainerChecked)
                {
                    // Ensure the container exists.
                    var response = Database.Value.CreateContainerIfNotExistsAsync(
                        containerProperties
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode)
                    {
                        // Panic!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CosmoDbCrudRepository_CreateContainer,
                                ContainerName,
                                response.StatusCode,
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
                ).Select(x => x.model); // We want the inner model.
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> AddAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Is the key missing?
                if (KeyUtility.IsKeyMissing(model.Key))
                {
                    // Create a new random key value.
                    model.Key = KeyUtility.CreateRandomKey<TKey>();
                }
                
                // Wrap the model up.
                var wrapper = new CosmoDbWrapper<TModel>()
                {
                    id = $"{model.Key}",
                    model = model
                };

                // Defer to the CosmoDb container.
                var newModel = await Container.Value.CreateItemAsync(
                    item: wrapper,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);
                
                // Return the result.
                return newModel.Resource.model;
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_AddAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> UpdateAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            // Wrap the model up.
            var wrapper = new CosmoDbWrapper<TModel>()
            {
                id = $"{model.Key}",
                model = model
            };

            try
            {
                // Defer to the CosmoDb container.
                var newModel = await Container.Value.ReplaceItemAsync(
                    item: wrapper,
                    id: $"{model.Key}",
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);

                // Return the result.
                return newModel.Resource.model;

            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_UpdateAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task DeleteAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Defer to the CosmoDb container.
                await Container.Value.DeleteItemAsync<TModel>(
                    id: $"{model.Key}",
                    partitionKey: PartitionKey.None,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_DeleteAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        #endregion

        // *******************************************************************
        // Protected methods.
        // *******************************************************************

        #region Protected methods

        /// <inheritdoc />
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


    /// <summary>
    /// This class is a base CosmoDb implementation of the <see cref="ICrudRepository{TModel, TKey1, TKey2}"/>
    /// interface.
    /// </summary>
    /// <typeparam name="TOptions">The options type associated with the repository.</typeparam>
    /// <typeparam name="TModel">The type of associated model.</typeparam>
    /// <typeparam name="TKey1">The key 1 type associated with the model.</typeparam>
    /// <typeparam name="TKey2">The key 2 type associated with the model.</typeparam>
    public abstract class CosmoDbCrudRepositoryBase<TOptions, TModel, TKey1, TKey2> :
        CrudRepositoryBase<TOptions, TModel, TKey1, TKey2>,
        ICrudRepository<TModel, TKey1, TKey2>
        where TModel : class, IModel<TKey1, TKey2>
        where TOptions : IOptions<CosmoDbRepositoryOptions>
        where TKey1 : new()
        where TKey2 : new()
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
        /// This constructor creates a new instance of the <see cref="CosmoDbCrudRepositoryBase{TOptions, TModel, TKey1, TKey2}"/>
        /// class.
        /// </summary>
        /// <param name="options">The options to use for the repository.</param>
        /// <param name="client">The CosmoDb client to use with the repository.</param>
        protected CosmoDbCrudRepositoryBase(
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
                    // Ensure the database exists.
                    var response = Client.CreateDatabaseIfNotExistsAsync(
                        Options.Value.DatabaseId
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode)
                    {
                        // Panik!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CreateDatabase,
                                Options.Value.DatabaseId,
                                response.StatusCode,
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
                    PartitionKeyPath = "/id",
                    IndexingPolicy = new IndexingPolicy()
                    {
                        Automatic = false,
                        IndexingMode = IndexingMode.Lazy,
                    }
                };

                // Should we check the underyling container?
                if (false == ContainerChecked)
                {
                    // Ensure the container exists.
                    var response = Database.Value.CreateContainerIfNotExistsAsync(
                        containerProperties
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode)
                    {
                        // Panic!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CosmoDbCrudRepository_CreateContainer,
                                ContainerName,
                                response.StatusCode,
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
                ).Select(x => x.model); // We want the inner model.
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> AddAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Wrap the model up.
                var wrapper = new CosmoDbWrapper<TModel>()
                {
                    id = $"{model.Key1}|{model.Key2}",
                    model = model
                };

                // Defer to the CosmoDb container.
                var newModel = await Container.Value.CreateItemAsync(
                    item: wrapper,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);

                // Return the result.
                return newModel.Resource.model;
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_AddAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> UpdateAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            // Wrap the model up.
            var wrapper = new CosmoDbWrapper<TModel>()
            {
                id = $"{model.Key1}|{model.Key2}",
                model = model
            };

            try
            {
                // Defer to the CosmoDb container.
                var newModel = await Container.Value.ReplaceItemAsync(
                    item: wrapper,
                    id: $"{model.Key1}|{model.Key2}",
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);

                // Return the result.
                return newModel.Resource.model;

            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_UpdateAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task DeleteAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Defer to the CosmoDb container.
                await Container.Value.DeleteItemAsync<TModel>(
                    id: $"{model.Key1}|{model.Key2}",
                    partitionKey: PartitionKey.None,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_DeleteAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        #endregion

        // *******************************************************************
        // Protected methods.
        // *******************************************************************

        #region Protected methods

        /// <inheritdoc />
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



    /// <summary>
    /// This class is a base CosmoDb implementation of the <see cref="ICrudRepository{TModel, TKey1, TKey2, TKey3}"/>
    /// interface.
    /// </summary>
    /// <typeparam name="TOptions">The options type associated with the repository.</typeparam>
    /// <typeparam name="TModel">The type of associated model.</typeparam>
    /// <typeparam name="TKey1">The key 1 type associated with the model.</typeparam>
    /// <typeparam name="TKey2">The key 2 type associated with the model.</typeparam>
    /// <typeparam name="TKey3">The key 2 type associated with the model.</typeparam>
    public abstract class CosmoDbCrudRepositoryBase<TOptions, TModel, TKey1, TKey2, TKey3> :
        CrudRepositoryBase<TOptions, TModel, TKey1, TKey2, TKey3>,
        ICrudRepository<TModel, TKey1, TKey2, TKey3>
        where TModel : class, IModel<TKey1, TKey2, TKey3>
        where TOptions : IOptions<CosmoDbRepositoryOptions>
        where TKey1 : new()
        where TKey2 : new()
        where TKey3 : new()
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
        /// This constructor creates a new instance of the <see cref="CosmoDbCrudRepositoryBase{TOptions, TModel, TKey1, TKey2, TKey3}"/>
        /// class.
        /// </summary>
        /// <param name="options">The options to use for the repository.</param>
        /// <param name="client">The CosmoDb client to use with the repository.</param>
        protected CosmoDbCrudRepositoryBase(
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
                    // Ensure the database exists.
                    var response = Client.CreateDatabaseIfNotExistsAsync(
                        Options.Value.DatabaseId
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode)
                    {
                        // Panik!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CreateDatabase,
                                Options.Value.DatabaseId,
                                response.StatusCode,
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
                    PartitionKeyPath = "/id",
                    IndexingPolicy = new IndexingPolicy()
                    {
                        Automatic = false,
                        IndexingMode = IndexingMode.Lazy,
                    }
                };

                // Should we check the underyling container?
                if (false == ContainerChecked)
                {
                    // Ensure the container exists.
                    var response = Database.Value.CreateContainerIfNotExistsAsync(
                        containerProperties
                        ).Result;

                    // Did the call fail?
                    if (System.Net.HttpStatusCode.OK != response.StatusCode &&
                        System.Net.HttpStatusCode.Accepted != response.StatusCode &&
                        System.Net.HttpStatusCode.Created != response.StatusCode)
                    {
                        // Panic!
                        throw new InvalidOperationException(
                            message: string.Format(
                                Resources.CosmoDbCrudRepository_CreateContainer,
                                ContainerName,
                                response.StatusCode,
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
                ).Select(x => x.model); // We want the inner model.
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> AddAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Wrap the model up.
                var wrapper = new CosmoDbWrapper<TModel>()
                {
                    id = $"{model.Key1}|{model.Key2}|{model.Key3}",
                    model = model
                };

                // Defer to the CosmoDb container.
                var newModel = await Container.Value.CreateItemAsync(
                    item: wrapper,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);

                // Return the result.
                return newModel.Resource.model;
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_AddAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task<TModel> UpdateAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            // Wrap the model up.
            var wrapper = new CosmoDbWrapper<TModel>()
            {
                id = $"{model.Key1}|{model.Key2}|{model.Key3}",
                model = model
            };

            try
            {
                // Defer to the CosmoDb container.
                var newModel = await Container.Value.ReplaceItemAsync(
                    item: wrapper,
                    id: $"{model.Key1}{model.Key2}|{model.Key3}",
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);

                // Return the result.
                return newModel.Resource.model;

            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_UpdateAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        // *******************************************************************

        /// <inheritdoc />
        public override async Task DeleteAsync(
            TModel model,
            CancellationToken cancellationToken = default
            )
        {
            // Validate the parameters before attempting to use them.
            Guard.Instance().ThrowIfNull(model, nameof(model));

            try
            {
                // Defer to the CosmoDb container.
                await Container.Value.DeleteItemAsync<TModel>(
                    id: $"{model.Key1}|{model.Key2}|{model.Key3}",
                    partitionKey: PartitionKey.None,
                    cancellationToken: cancellationToken
                    ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Add better context to the error.
                throw new RepositoryException(
                    message: string.Format(
                        Resources.CosmoDbCrudRepository_DeleteAsync,
                        GetType().Name,
                        typeof(TModel).Name,
                        JsonSerializer.Serialize(model)
                        ),
                    innerException: ex
                    );
            }
        }

        #endregion

        // *******************************************************************
        // Protected methods.
        // *******************************************************************

        #region Protected methods

        /// <inheritdoc />
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
