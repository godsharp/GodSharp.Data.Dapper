using System.Linq;

namespace GodSharp.Data.Dapper.Extension
{
    /// <summary>
    /// String Extension
    /// </summary>
    public class StringEx
    {
#if NET35
        internal static bool IsNullOrWhiteSpace(string value)
        {
            if (value == null)
            {
                return true;
            }

            return value.All(char.IsWhiteSpace);
        } 
#endif
    }
}
