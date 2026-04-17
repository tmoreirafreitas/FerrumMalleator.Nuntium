namespace FerrumMalleator.Nuntium.Configuration
{
    public class MessagingOptions
    {
        public PersistenceMode PersistenceMode { get; set; } = PersistenceMode.InMemory;
        public bool EnableSaga { get; set; } = false;
        public bool EnableOutbox { get; set; } = false;
    }

    public enum PersistenceMode
    {
        InMemory,
        EntityFramework
    }
}
