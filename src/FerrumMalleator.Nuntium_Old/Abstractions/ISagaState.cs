using System;

namespace FerrumMalleator.Nuntium.Abstractions
{
    public interface ISagaState
    {
        Guid CorrelationId { get; set; }
    }
}
