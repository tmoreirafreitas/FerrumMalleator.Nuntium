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

## ⚠️ Status do Projeto

> 🚧 **Versão 0.x (Early Release)**
> A arquitetura principal está estável, mas a API pode evoluir com base no feedback da comunidade antes da versão 1.0.0.

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
    })
    .UseInMemory()
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

### 2. Publicando mensagens

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

### 3. Consumindo mensagens

```csharp
public class PedidoCriadoConsumer : IMessageConsumer<PedidoCriado>
{
    public Task ConsumeAsync(PedidoCriado message, CancellationToken ct)
    {
        Console.WriteLine($"Pedido recebido: {message.Id}");
        return Task.CompletedTask;
    }
}
```

---

## 📦 Samples

O projeto possui exemplos práticos no projeto:

`FerrumMalleator.Nuntium.Samples`

Os samples demonstram cenários comuns de mensageria:

* Producer / Consumer
* Retry e DLQ
* Outbox Pattern
* Saga
* Transporte e persistência InMemory
* Integração com Entity Framework

O objetivo é permitir entendimento rápido da arquitetura e facilitar a adoção do framework.

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

## 🧠 Observabilidade (Roadmap)

* [ ] Integração com OpenTelemetry (emissão de traces, métricas e logs)
> O destino dos dados (Prometheus, Jaeger, Elastic, etc.) é definido pela aplicação.

---

## 🧠 Integração natural com CQRS

```csharp
IRequestHandler<T>     → MediatR
IMessageConsumer<T>    → Nuntium
```

Se você já usa MediatR, você já sabe usar Nuntium.

---

## 🔮 Roadmap

* [ ] Observabilidade completa
* [ ] Suporte a RabbitMQ
* [ ] Registro automático de consumers via assembly scanning (opcional)

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

---

## 📄 Licença

MIT
