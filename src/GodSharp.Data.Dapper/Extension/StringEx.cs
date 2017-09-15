#if NET35
using System.Linq;

namespace GodSharp.Data.Dapper.Extension
{
    /// <summary>
    /// String Extension
    /// </summary>
    public class StringEx
    {
        internal static bool IsNullOrWhiteSpace(string value)
        {
            if (value == null)
            {
                return true;
            }

            return value.All(char.IsWhiteSpace);
        } 
    }
}
#endif
