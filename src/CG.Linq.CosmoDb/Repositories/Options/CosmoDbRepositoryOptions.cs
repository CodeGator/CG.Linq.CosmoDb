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

        #endregion
    }
}
