namespace ParquetSharpLINQ.Constants
{
    /// <summary>
    ///     Constants used for partition handling throughout the ParquetSharpLINQ library.
    /// </summary>
    public static class PartitionConstants
    {
        /// <summary>
        ///     Prefix used to identify partition values in row dictionaries.
        ///     Uses null byte character (\0) which cannot appear in valid Parquet column names,
        ///     guaranteeing zero possibility of collision with actual column data.
        /// </summary>
        public const string PartitionPrefix = "\0";
    }
}