![NuGet](https://img.shields.io/nuget/v/FerrumMalleator.Nuntium)
![License](https://img.shields.io/badge/license-MIT-blue)
![Build](https://img.shields.io/github/actions/workflow/status/tmoreirafreitas/FerrumMalleator.Nuntium/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=tmoreirafreitas_FerrumMalleator.Nuntium\&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=tmoreirafreitas_FerrumMalleator.Nuntium)

# ⚒️ FerrumMalleator.Nuntium

> **Mensageria moderna, poderosa e simples — do jeito que deveria ser.**

FerrumMalleator.Nuntium é um framework de mensageria leve, extensível e orientado a handlers para .NET, criado para aplicações modernas baseadas em eventos, CQRS e microserviços.

Ele nasce de uma ideia simples:

> **Mensageria não deveria ser difícil.**

---

## ✨ Por que o Nuntium existe?

Frameworks de mensageria atuais são poderosos —
mas frequentemente vêm com um custo:

```text
Complexidade desnecessária
Curva de aprendizado alta
Muito boilerplate
```

O Nuntium foi criado para resolver isso.

---

## 🧠 Princípio central

> **Mensageria deve ser poderosa — mas simples de usar.**

Sem mágica.
Sem excesso de abstração.
Sem sofrimento.

---

## ✔ Principais características

🧠 **Orientado a Handlers** (estilo MediatR)  
⚡ **Configuração fluida e intuitiva**  
🔌 **Transporte desacoplado (Kafka, InMemory e outros futuros)**  
💾 **Outbox integrado (consistência garantida)**  
🔁 **Idempotência nativa**  
🔗 **Saga simplificada (orquestração distribuída)**  
☠️ **Dead Letter Queue (DLQ) com reprocessamento automático e backoff exponencial**  
🔄 **Retry com política configurável**  
🧩 **Arquitetura extensível**  

---

## 🧠 Filosofia

O Nuntium não tenta ser um framework enterprise pesado.

Ele segue três princípios fundamentais:

```text
1. Simplicidade > Complexidade
2. Código explícito > Magia
3. Produtividade > Configuração
```

---

## 🚀 Quick Start

### 1. Configuração

```csharp
builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = "localhost:9092";
    });

    bus.ScanConsumersFromAssembly<Program>();
});
```

---

### 2. Mapeamento de tópico

```csharp
using FerrumMalleator.Nuntium.Attributes;

[Topic("pedido.criado", "pedido-group")]
public sealed record PedidoCriado(Guid Id);
```

---

### 3. Consumindo mensagens

```csharp
public sealed class PedidoCriadoConsumer
    : IMessageConsumer<PedidoCriado>
{
    public Task ConsumeAsync(
        PedidoCriado message,
        CancellationToken ct)
    {
        Console.WriteLine(
            $"Pedido recebido: {message.Id}");

        return Task.CompletedTask;
    }
}
```

---

### 4. Publicando mensagens

```csharp
public class PedidoService(IMessagePublisher publisher)
{
    public async Task CriarPedido()
    {
        var pedido = new PedidoCriado(Guid.NewGuid());

        await publisher.PublishAsync(pedido);
    }
}
```

---

### 5. Configuração explícita (opcional)

O Nuntium também permite configuração totalmente explícita utilizando a API fluente tradicional.

```csharp
builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = "localhost:9092";
    });

    bus.WithTopic<PedidoCriado>(
        "pedido.criado",
        "pedido-group");

    bus.AddConsumer<
        PedidoCriadoConsumer,
        PedidoCriado>();
});
```

Essa abordagem oferece:

- controle explícito de tópicos
- configuração centralizada
- ausência de attributes
- maior previsibilidade arquitetural

---

## 📦 Samples

O Nuntium inclui samples oficiais demonstrando os principais cenários do framework.

| Sample | Descrição |
|---|---|
| [BasicFlow](./samples/FerrumMalleator.Nuntium.Sample.BasicFlow/README.md) | Producer + Consumer + Kafka + Observabilidade |
| [SagaFlow](./samples/FerrumMalleator.Nuntium.Sample.SagaFlow/README.md) | Saga orchestration + Workflow distribuído |
| [OutboxFlow](./samples/FerrumMalleator.Nuntium.Sample.OutboxFlow/README.md) | Outbox Pattern + Eventual consistency |

Os samples demonstram:

- Kafka real
- OpenTelemetry-compatible instrumentation
- Distributed tracing
- Metrics
- Saga orchestration
- Outbox Pattern
- Eventual consistency
- Entity Framework Core InMemory

---

## 🧱 Transporte vs Persistência

O Nuntium separa claramente duas responsabilidades fundamentais:

```text
Transporte  → como a mensagem é enviada (Kafka, InMemory, etc)
Persistência → onde estados e mensagens são armazenados (EF Core, InMemory)
```

Essa separação permite maior flexibilidade, clareza e previsibilidade na configuração.

---

### 1. Produção (Kafka + EF Core)
```csharp
builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = "localhost:9092";
    })
    .UseEfCorePersistence<SampleDbContext>(opt =>
    {
        opt.UseSqlServer("connection-string");
    })
    .UseOutbox()
    .UseRetry(r => r.MaxAttempts = 3)
    .AddSaga()
    .WithTopic<PedidoCriado>("pedido.criado", "pedido-group")
    .AddConsumer<PedidoCriadoConsumer, PedidoCriado>();
});
```

---

### 2. Testes / Desenvolvimento (InMemory)

```csharp
builder.Services.AddNuntium(bus =>
{
    bus.UseInMemoryTransport()
       .UseInMemoryPersistence()
       .UseOutbox()
       .UseRetry(r => r.MaxAttempts = 3)
       .AddSaga()
       .WithTopic<PedidoCriado>("pedido.criado", "pedido-group")
       .AddConsumer<PedidoCriadoConsumer, PedidoCriado>();
});
```

---

### 3. Compatibilidade
O método .UseInMemory() continua disponível e equivale a:

```csharp
.UseInMemoryTransport()
.UseInMemoryPersistence()
```

---


## 🔁 Outbox (Consistência garantida)

```text
Aplicação → Outbox → Transporte → Consumer
```

✔ nenhuma mensagem perdida  
✔ consistência entre banco e mensageria  
✔ tolerância a falhas  

---

## ☠️ Dead Letter Queue (DLQ)

```text
Falha → DLQ → Reprocessamento automático → Recuperação
```

✔ retry automático  
✔ backoff exponencial  
✔ controle de tentativas  
✔ reenvio transparente  

---

## 🔗 Saga (Orquestração simplificada)

```csharp
public class PedidoSaga :
    ISagaHandler<PedidoCriado, PedidoState>,
    ISagaHandler<PagamentoAprovado, PedidoState>
{
    public Task HandleAsync(PedidoCriado message, PedidoState state, CancellationToken ct)
    {
        state.Criado = true;
        return Task.CompletedTask;
    }

    public Task HandleAsync(PagamentoAprovado message, PedidoState state, CancellationToken ct)
    {
        state.Pago = true;
        return Task.CompletedTask;
    }
}
```

✔ correlação automática (`Id`, `*Id`, `CorrelationId`)  
✔ estado persistido automaticamente  
✔ integração com retry e DLQ

---

## 🧱 Persistência com Entity Framework

Para ambientes de produção, utilize persistência com Entity Framework:

```csharp
builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = "localhost:9092";
    })
    .UseEfCorePersistence<SampleDbContext>(opt =>
    {
        opt.UseInMemoryDatabase("nuntium"); // ou UseSqlServer / UseNpgsql
    })
    .UseOutbox()
    .UseRetry(r =>
    {
        r.MaxAttempts = 3;
    })
    .AddSaga()
    .WithTopic<PedidoCriado>("pedido.criado", "pedido-group")
    .AddConsumer<PedidoCriadoConsumer, PedidoCriado>();
});
```
---

## 🔄 Arquitetura

```text
IMessagePublisher → (Outbox ou direto)
                  ↓
          IMessageTransport
                  ↓
           Kafka / RabbitMQ / etc
                  ↓
        Dispatcher → Consumer / Saga
```

---

## 🧩 Transportes

Suporte planejado para múltiplos transports:

* Kafka (atual)
* RabbitMQ (futuro)
* ActiveMQ (futuro)

Sem alterar o código da aplicação.

---

## 🧩 Extensibilidade

O Nuntium segue um princípio simples:

> **Convention first, extensibility by contract.**

O framework possui comportamentos padrão prontos para uso —
mas os principais componentes são baseados em contratos públicos.

Isso permite substituir implementações internas apenas registrando novas implementações no container de DI.

---

### Exemplo: substituindo o transporte

```csharp
services.AddSingleton<IMessageTransport, CustomTransport>();
```
   
---

### Componentes extensíveis

Os principais contratos públicos incluem:

* `IMessageTransport`
* `IMessagePublisher`
* `IRetryExecutor`
* `IOutboxStore`
* `IDeadLetterStore`
* `ISagaHandler<TMessage, TState>`

---

### Objetivo

O objetivo é permitir customização sem transformar o framework em uma caixa-preta complexa.

```text
Comportamentos padrão quando possível
Customização quando necessário
```

* Sem reflection excessiva.
* Sem XML.
* Sem configuração proprietária.

---

## ✨ Instrumentação OpenTelemetry-compatible

O Nuntium agora possui instrumentação compatível com OpenTelemetry:

- Distributed tracing
- Metrics
- Kafka instrumentation
- Saga instrumentation
- Outbox observability
- DeadLetter observability
- Retry tracing
- Semantic messaging tags

Compatível com:
- OpenTelemetry
- Grafana
- Jaeger
- Prometheus
- Seq
- OTLP exporters  

> O destino dos dados (Prometheus, Jaeger, Elastic, etc.) é definido pela aplicação.

---

## ⚠️ Registro de mensagens

Cada tipo de mensagem deve ser registrado apenas uma vez utilizando:

- `WithTopic<T>()`
- `TopicAttribute`

O Nuntium utiliza um registro único de metadata por mensagem para:

- resolução de tópicos
- serialização
- roteamento
- observabilidade
- particionamento

---

## 🧠 Integração natural com CQRS

```csharp
IRequestHandler<T>     → MediatR
IMessageConsumer<T>    → Nuntium
```

Se você já usa MediatR, você já sabe usar Nuntium.

---

## 🔮 Roadmap

O roadmap do Nuntium prioriza:

- simplicidade operacional
- experiência do desenvolvedor (DX)
- estabilidade da API pública
- evolução sustentável do ecossistema

---

### ✅ v1.1.0 — Developer Experience & Adoption

Entregue:

- [x] Registro automático de consumers via assembly scanning
- [x] Descoberta automática de `IMessageConsumer<T>`
- [x] Attribute-based topic mapping
- [x] Expansão da documentação oficial
- [x] Projetos de exemplo completos
- [x] Stack Docker Compose para desenvolvimento local

### 🧩 v1.2.0 — Enterprise Messaging Features

Foco em integração enterprise e mensageria distribuída avançada.

#### Planejado

- [ ] Message Headers
- [ ] Correlation / Causation metadata
- [ ] Distributed Trace Context propagation
- [ ] Retry policies por mensagem/consumer
- [ ] Delayed / Scheduled messages
- [ ] Estratégias avançadas de DLQ e poison messages

---

### 🚚 v1.3.0 — Transport Extensibility

Foco em expansão do ecossistema de transportes.

#### Planejado

- [ ] `FerrumMalleator.Nuntium.Transport.RabbitMq`
- [ ] Possível suporte futuro a ActiveMq e Azure Service Bus

> O Nuntium permanece Kafka-native no core principal.

---

### 🏗️ v2.0.0 — Broker-Agnostic Architecture (Possibilidade futura)

Avaliação futura baseada na maturidade do ecossistema e necessidade real de desacoplamento completo de transportes.

#### Possibilidades

- [ ] Core totalmente broker-agnostic
- [ ] Kafka desacoplado do core
- [ ] Pipeline de transporte unificado
- [ ] Estratégias avançadas de particionamento
- [ ] Orquestração distribuída avançada

> Esta seção representa possibilidades arquiteturais futuras, não um compromisso de implementação.

---

### 🧠 Filosofia de evolução

O objetivo do Nuntium não é crescer rapidamente em quantidade de features.

O foco principal permanece:

```text
simplicidade
clareza
baixo acoplamento
produtividade
```

---

## ⚒️ Sobre o FerrumMalleator

FerrumMalleator é um conjunto de ferramentas open source focadas em produtividade e arquitetura para .NET.

O Nuntium é a primeira dessas ferramentas.

Outras soluções planejadas:

📦 FerrumMalleator.Mediator  
🔑 FerrumMalleator.Identifiers (IDs determinísticos)  
🧰 Utilitários do dia a dia  

---

## 🤝 Contribuição

Contribuições são bem-vindas!

Abra uma issue ou pull request.
Abra uma issue ou pull request.

---

## 📄 Licença

MIT
