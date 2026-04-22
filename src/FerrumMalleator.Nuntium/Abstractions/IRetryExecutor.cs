namespace FerrumMalleator.Nuntium.Abstractions
{
    public interface IRetryExecutor
    {
        Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure);
    }
}
