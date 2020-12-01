using CG.Business.Models;
using System;

namespace CG.Linq.CosmoDb.Repositories
{
    /// <summary>
    /// This class is used to internally wrap models with, since CosmoDb 
    /// requires any row to have an 'id' property, with a string type, and 
    /// extending that requirement to the <typeparamref name="TModel"/> type
    /// seems ... Excessive.
    /// </summary>
    internal class CosmoDbWrapper<TModel>
        where TModel : class, IModel
    {
        // *******************************************************************
        // Properties.
        // *******************************************************************

        #region Properties

        /// <summary>
        /// This property is required by CosmoDb.
        /// </summary>
        public string id { get; set; }

        /// <summary>
        /// This property contains the wrapped model instance.
        /// </summary>
        public TModel model { get; set; }

        #endregion
    }
}
