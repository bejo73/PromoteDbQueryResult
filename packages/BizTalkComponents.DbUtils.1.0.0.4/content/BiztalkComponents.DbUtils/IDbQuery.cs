using System;

namespace BizTalkComponents.DbUtils
{
    public interface IDbQuery
    {
        int ExecuteScalar(string query);
    }
}
