using CG.Business.Repositories.Options;
using CG.Linq.CosmoDb.Properties;
using System;
using System.ComponentModel.DataAnnotations;

namespace CG.Linq.CosmoDb.Repositories.Options
{
    /// <summary>
    /// This class contains configuration settings for a CosmoDb repository.
    /// </summary>
    public class CosmoDbRepositoryOptions : LinqRepositoryOptions
    {
        // *******************************************************************
        // Properties.
        // *******************************************************************

        #region Properties

        /// <summary>
        /// This property contains the database identifier to use with the repository.
        /// </summary>
        [Required(ErrorMessageResourceName = "CosmoRepositoryOptions_DbId",
                  ErrorMessageResourceType = typeof(Resources))]
        public string DatabaseId { get; set; }

        /// <summary>
        /// This property indicates whether the database should be created, if
        /// needed, at startup (or not). Note, this step is only ever performed
        /// when running in the <c>Development</c> environment, in order to 
        /// prevent horrible accidents in production.
        /// </summary>
        public bool EnsureCreated { get; set; }

        /// <summary>
        /// This property indicates whether the database should be dropped, if 
        /// it already exists (or not). Note, this step is only ever performed
        /// when running in the <c>Development</c> environment, in order to 
        /// prevent horrible accidents in production.
        /// </summary>
        public bool DropDatabase { get; set; }

        /// <summary>
        /// This property indicates whether the database should be seeded with 
        /// data, if needed, at startup (or not). Note, this step is only ever 
        /// performed when running in the <c>Development</c> environment, in order 
        /// to prevent horrible accidents in production.
        /// </summary>
        public bool SeedDatabase { get; set; }

        #endregion
    }
}
