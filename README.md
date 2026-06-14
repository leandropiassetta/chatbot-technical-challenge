# Avaliação Técnica - Desenvolvedor Go | Python | Ruby + IA

Autor: Leandro Piassetta

## Premissas gerais

- A fila mencionada no desafio é uma SQS Standard, portanto o consumo segue semântica de entrega `at-least-once`.
- A base oficial de políticas de contestação/reembolso é a fonte de verdade para respostas do chatbot.
- O modelo principal é um LLM generativo consumido via provedor gerenciado, mas a solução não depende de recurso proprietário específico.
- Quando um ponto do enunciado estiver ambíguo, a resposta explicita a interpretação adotada e segue com uma solução de produção.

## Como validar o código

O Exercício 1 também foi implementado como código Go compilável e testável:

```text
├── README.md
├── go.mod
└── exercicio1/
    ├── main.go
    └── main_test.go
```

Para validar:

```bash
go test ./...
```

Os testes usam um fake SQS em memória, portanto `go test ./...` não exige credenciais AWS nem uma fila real. Para executar o binário contra uma fila real, é necessário configurar `QUEUE_URL` e credenciais AWS válidas no ambiente.

Para compilar o worker como binário local:

```bash
go build -o /tmp/exercicio1-worker ./exercicio1
```

## Exercício 1

### Diagnóstico do worker atual

O problema principal não é o loop infinito em si, mas o fato de o throughput efetivo do worker ser menor do que a taxa de chegada das mensagens durante picos. O código atual processa apenas uma mensagem por chamada de `ReceiveMessage`, faz o processamento de forma serial, não usa long polling, ignora erros da AWS SDK, não tem controle de shutdown e apaga mensagens mesmo quando `handleMessage` falha.

Pontos críticos encontrados:

- `ReceiveMessage` ignora erro; se a AWS retornar erro, throttling ou timeout, o worker não reage corretamente.
- `MaxNumberOfMessages: 1` limita artificialmente o throughput. A SQS permite receber até 10 mensagens por chamada.
- Falta `WaitTimeSeconds`, então o worker usa short polling, aumenta custo de API call e pode fazer polling ineficiente.
- Falta `VisibilityTimeout` explícito alinhado ao tempo real de processamento.
- O processamento é serial. Se `handleMessage` demora por banco, enriquecimento ou logging, a fila acumula.
- A mensagem é apagada mesmo quando `handleMessage` retorna erro, causando perda silenciosa de eventos.
- `DeleteMessage` também ignora erro, então duplicidades podem ocorrer sem visibilidade operacional.
- `context.Background()` impede shutdown gracioso em ECS/Kubernetes.
- Não há backoff em erro de receive/delete nem proteção contra throttling.
- Não há logs estruturados, métricas ou contadores por tipo de falha.
- Não há estratégia explícita de DLQ, `maxReceiveCount` e idempotência.

### Versão corrigida em Go

O código abaixo separa o cliente SQS em uma interface testável, recebe mensagens em lote, usa long polling, aplica concorrência controlada, respeita cancelamento por `context`, apaga a mensagem somente após sucesso e deixa claro onde a idempotência deve existir.

Dependências externas: Go 1.24+ e AWS SDK for Go v2, conforme versões fixadas no `go.mod`. A versão do Go segue o `go.mod` do projeto para garantir reprodutibilidade dos testes. A versão completa está em `exercicio1/main.go`, com inicialização do client SQS, leitura de `QUEUE_URL` e testes automatizados em `exercicio1/main_test.go`.

O bloco abaixo mostra o núcleo testável do worker; o arquivo `exercicio1/main.go` contém também o `main` executável.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type HandlerFunc func(ctx context.Context, body string) error

type WorkerConfig struct {
	QueueURL           string
	MaxMessages        int32
	WaitTimeSeconds    int32
	VisibilityTimeout  int32
	Concurrency        int
	HandlerTimeout     time.Duration
	ReceiveBackoff     time.Duration
	MaxReceiveBackoff  time.Duration
	DeleteCallTimeout  time.Duration
}

type Worker struct {
	client  SQSAPI
	handler HandlerFunc
	cfg     WorkerConfig
	logger  *slog.Logger
}

func NewWorker(client SQSAPI, cfg WorkerConfig, handler HandlerFunc, logger *slog.Logger) (*Worker, error) {
	if client == nil {
		return nil, errors.New("sqs client is required")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		client:  client,
		handler: handler,
		cfg:     cfg,
		logger:  logger,
	}, nil
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.MaxMessages <= 0 || c.MaxMessages > 10 {
		c.MaxMessages = 10
	}
	if c.WaitTimeSeconds <= 0 || c.WaitTimeSeconds > 20 {
		c.WaitTimeSeconds = 20
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 60
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.ReceiveBackoff <= 0 {
		c.ReceiveBackoff = 500 * time.Millisecond
	}
	if c.MaxReceiveBackoff <= 0 {
		c.MaxReceiveBackoff = 10 * time.Second
	}
	if c.DeleteCallTimeout <= 0 {
		c.DeleteCallTimeout = 5 * time.Second
	}
	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = time.Duration(c.VisibilityTimeout)*time.Second - 5*time.Second
	}
	return c
}

func (c WorkerConfig) validate() error {
	if c.QueueURL == "" {
		return errors.New("queue URL is required")
	}
	if c.HandlerTimeout <= 0 {
		return errors.New("handler timeout must be positive")
	}
	if c.HandlerTimeout >= time.Duration(c.VisibilityTimeout)*time.Second {
		return fmt.Errorf("handler timeout %s must be lower than visibility timeout %ds", c.HandlerTimeout, c.VisibilityTimeout)
	}
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	slots := make(chan struct{}, w.cfg.Concurrency)
	backoff := w.cfg.ReceiveBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		availableSlots := acquireSlots(ctx, slots, int(w.cfg.MaxMessages))
		if availableSlots == 0 {
			return nil
		}

		out, err := w.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(w.cfg.QueueURL),
			MaxNumberOfMessages:         int32(availableSlots),
			WaitTimeSeconds:             w.cfg.WaitTimeSeconds,
			VisibilityTimeout:           w.cfg.VisibilityTimeout,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			releaseSlots(slots, availableSlots)
			if ctx.Err() != nil {
				return nil
			}

			wait := withJitter(backoff)
			w.logger.Error("failed to receive SQS messages", "error", err, "backoff", wait)

			if !sleep(ctx, wait) {
				return nil
			}
			backoff = minDuration(backoff*2, w.cfg.MaxReceiveBackoff)
			continue
		}

		backoff = w.cfg.ReceiveBackoff
		unusedSlots := availableSlots - len(out.Messages)
		if unusedSlots > 0 {
			releaseSlots(slots, unusedSlots)
		}

		for _, msg := range out.Messages {
			wg.Add(1)
			go func(msg types.Message) {
				defer wg.Done()
				defer releaseSlots(slots, 1)
				w.processOne(ctx, msg)
			}(msg)
		}
	}
}

func (w *Worker) processOne(ctx context.Context, msg types.Message) {
	messageID := aws.ToString(msg.MessageId)
	receiptHandle := aws.ToString(msg.ReceiptHandle)
	if receiptHandle == "" {
		w.logger.Error("message without receipt handle", "message_id", messageID)
		return
	}

	handlerCtx, cancel := context.WithTimeout(ctx, w.cfg.HandlerTimeout)
	defer cancel()

	if err := w.handler(handlerCtx, aws.ToString(msg.Body)); err != nil {
		w.logger.Error(
			"message handling failed",
			"error", err,
			"message_id", messageID,
			"receive_count", msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)],
		)
		return
	}

	deleteCtx, cancelDelete := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.DeleteCallTimeout)
	defer cancelDelete()

	if _, err := w.client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.cfg.QueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		w.logger.Error("failed to delete SQS message", "error", err, "message_id", messageID)
		return
	}

	w.logger.Info("message processed", "message_id", messageID)
}

func acquireSlots(ctx context.Context, slots chan struct{}, limit int) int {
	acquired := 0
	for acquired < limit {
		select {
		case slots <- struct{}{}:
			acquired++
		case <-ctx.Done():
			releaseSlots(slots, acquired)
			return 0
		default:
			if acquired > 0 {
				return acquired
			}

			select {
			case slots <- struct{}{}:
				acquired++
			case <-ctx.Done():
				return 0
			}
		}
	}
	return acquired
}

func releaseSlots(slots <-chan struct{}, count int) {
	for i := 0; i < count; i++ {
		<-slots
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func withJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
```

O worker não faz prefetch além da capacidade real de processamento: antes de chamar `ReceiveMessage`, ele reserva slots de concorrência e recebe no máximo a quantidade de slots livres. Isso evita que mensagens fiquem paradas em um buffer interno enquanto o `VisibilityTimeout` já está contando na SQS. Neste exemplo, o worker também usa `HandlerTimeout` menor que `VisibilityTimeout` para manter o processamento dentro da janela de visibilidade. Para workloads com duração imprevisível, eu adicionaria renovação periódica com `ChangeMessageVisibility`, mas deixaria isso fora do núcleo inicial para manter o contrato do worker simples e testável.

No `handleMessage`, eu colocaria a idempotência antes de qualquer efeito colateral. A chave principal deve ser uma chave de negócio, como `event_id`, ou no mínimo `session_id + event_type`. O `MessageId` da SQS ajuda na observabilidade, mas não deve ser a chave principal de deduplicação porque o mesmo evento de negócio pode ser republicado em outra mensagem.

Exemplo do padrão de idempotência:

```sql
INSERT INTO processed_events (event_id, processed_at)
VALUES (:event_id, now())
ON CONFLICT (event_id) DO NOTHING;
```

Se nenhuma linha for inserida, o evento já foi processado e o handler deve encerrar sem repetir efeitos colaterais. No shutdown, mensagens em processamento podem ser reentregues pela SQS; essa escolha é aceitável com entrega `at-least-once`, desde que a idempotência por `event_id` esteja correta.

### Respostas às perguntas de follow-up

O lag cresce durante picos porque a taxa de consumo é menor que a taxa de produção. Com apenas uma mensagem por receive e processamento serial, qualquer latência em banco, API externa ou enriquecimento derruba o throughput. O loop infinito só garante tentativa contínua, não garante capacidade. Em produção eu mediria `ApproximateAgeOfOldestMessage`, mensagens visíveis, mensagens em voo, duração do handler e taxa de erro por dependência.

Eu configuraria `WaitTimeSeconds` como 20 segundos para long polling. Isso reduz custo e chamadas vazias. O `VisibilityTimeout` deve ser maior que o p99 do processamento, com margem. Se o p99 do handler for 25 segundos, eu começaria com 60 segundos. Se o tempo for imprevisível, o worker deve renovar visibilidade com `ChangeMessageVisibility` enquanto processa, sempre com limite máximo para evitar mensagens presas indefinidamente.

Configuração esperada da fila:

- `ReceiveMessageWaitTimeSeconds`: 20 segundos.
- `VisibilityTimeout`: maior que o p99 do handler com margem, por exemplo 60 segundos se o p99 estiver até 25 segundos.
- `RedrivePolicy`: DLQ configurada com `maxReceiveCount` entre 3 e 5 e `deadLetterTargetArn` apontando para `session-events-dlq`.
- `MaxNumberOfMessages`: até 10, limitado pela quantidade de slots livres de processamento para evitar prefetch excessivo.

Em ECS com múltiplas tasks, o risco principal é duplicidade, porque SQS Standard entrega mensagens pelo menos uma vez. Também pode haver pressão excessiva em banco e APIs downstream se a escala subir sem limite. A mitigação é idempotência por `event_id`, controle de concorrência por task, autoscaling baseado em backlog/idade da fila, DLQ com `maxReceiveCount` e, se houver necessidade de ordem por sessão, avaliar SQS FIFO com `MessageGroupId=session_id`.

### Testes automatizados incluídos

- Quando o handler retorna sucesso, o worker chama `DeleteMessage`.
- Quando o handler retorna erro, o worker não chama `DeleteMessage`.
- Quando `ReceiveMessage` falha, o worker registra erro e aplica backoff.
- Quando o contexto é cancelado, o worker encerra sem vazar goroutines.
- Quando há mais mensagens que workers, a concorrência máxima configurada é respeitada.
- Quando todos os workers estão ocupados, o worker não faz novo `ReceiveMessage`, evitando prefetch que consome `VisibilityTimeout` parado em memória.
- Quando `DeleteMessage` falha, a falha é logada e a idempotência cobre o reprocessamento futuro.

### IA utilizada

Ferramentas usadas: ChatGPT e Codex (OpenAI).

Como usei: utilizei como apoio para revisar o diagnóstico do worker, organizar os riscos de SQS em produção e melhorar a clareza da explicação.

O que eu modifiquei/complementei manualmente: implementei o worker Go compilável, defini a interface mínima do cliente SQS, ajustei long polling, concorrência sem prefetch excessivo, backoff, timeouts, delete somente após sucesso e shutdown por contexto. Também escrevi testes com fake SQS para validar sucesso, erro no handler, retry no receive, falha no delete, cancelamento, concorrência e ausência de novo receive quando todos os slots estão ocupados.

Validação manual: rodei `go test ./...` e `go build`, e revisei as decisões considerando SQS Standard, entrega `at-least-once`, idempotência por chave de domínio e execução em ECS com múltiplas tasks.

## Exercício 2

### 2a — Estratégia para reduzir custo em pelo menos 50%

O aumento de aproximadamente 800 para 3.200 tokens por sessão indica que o sistema perdeu controle de orçamento de contexto. A estratégia correta não é apenas "resumir tudo", mas mudar o contrato de montagem do prompt: cada chamada deve receber somente a política relevante, o mínimo de histórico necessário e a mensagem atual.

Proposta de novo orçamento por chamada:

| Componente | Antes | Depois |
| --- | ---: | ---: |
| System prompt | ~1.200 tokens | até 300 tokens |
| Histórico | até 40 turnos | últimos 4 a 8 turnos, ~400 tokens |
| Base de conhecimento | 80 regras completas | top 3 trechos oficiais via RAG, ~500 a 700 tokens |
| Memória/sumário | não controlado | fatos estruturados, ~150 tokens |
| Mensagem atual | variável | ~100 tokens |
| Total estimado | ~3.200 tokens | ~1.350 a 1.650 tokens |

Isso reduz o input em aproximadamente 50% a 58% sem depender de trocar o modelo. A economia principal vem de limitar o contexto enviado ao modelo; cache entra como otimização de contexto, não como cache indiscriminado de respostas finais.

Mudanças técnicas:

- Classificar a intenção antes da chamada principal: contestação, fatura, saldo, produto, cobrança ou fallback. Essa classificação pode ser feita por regra, embedding ou classificador leve.
- Substituir o system prompt global de 80 regras por prompts de domínio. Para contestação, carregar apenas regras de contestação.
- Implementar RAG sobre a base oficial de políticas, com chunking pequeno e metadados por trecho: `doc_id`, `policy_version`, `score`, `effective_from`, `effective_to` e texto recuperado. Se não houver evidência suficiente ou a regra estiver fora de vigência, o chatbot deve usar fallback seguro.
- Usar sliding window para manter apenas os últimos turnos relevantes da conversa.
- Resumir histórico antigo em formato factual, por exemplo: `cliente perguntou sobre cobrança X`, `data informada Y`, `não houve autenticação`.
- Criar um token budget obrigatório no serviço que monta o prompt. Se passar do limite, cortar histórico antes de cortar regra oficial.
- Cachear embeddings, resultados de retrieval e blocos estáveis de contexto, sempre respeitando a versão vigente da política. Respostas finais personalizadas não devem ser cacheadas quando dependerem de conta, produto, autenticação, data da cobrança, país ou versão da política.
- Registrar `prompt_version`, `retrieved_doc_ids`, tokens de entrada/saída e custo estimado por sessão.

Trade-offs:

- Sliding window reduz custo, mas pode perder uma informação antiga importante. Por isso deve ser combinado com memória estruturada.
- Sumarização reduz tokens, mas pode distorcer fatos. O resumo deve ser factual e auditável, não interpretativo.
- RAG aumenta precisão e governança, mas adiciona latência e risco de retrieval errado. É obrigatório avaliar `retrieval_hit_rate`.
- Roteamento por intenção reduz contexto, mas erro de classificação pode carregar política errada. O fallback deve usar prompt mais conservador.
- Cache reduz custo e latência, mas cache de resposta final é arriscado em fintech. Duas perguntas parecidas podem exigir respostas diferentes por data, produto, autenticação ou política vigente.

### 2b — System prompt otimizado para contestação de cobranças

```text
Você é assistente financeiro da Empresa X para contestação de cobranças.

Use apenas CONTEXTO_OFICIAL e DADOS_DA_SESSÃO. Se faltar evidência, diga que não pode confirmar e encaminhe ao atendimento humano. Não invente prazos, valores, normas externas ou exceções.

Antes de responder, confira: intenção, data da cobrança, prazo/valor citado, evidência oficial e próxima ação segura. Não exponha essa verificação.

Regra oficial: contestação em até 60 dias da data da cobrança.

Responda em português, curto, objetivo e empático.

Exemplo:
Cliente: Posso contestar uma cobrança de 6 meses?
Resposta: Não. Pela política oficial, o prazo é de 60 dias da cobrança. Posso encaminhar o caso para análise do atendimento.

Cliente: Isso é regra do Banco Central?
Resposta: Não tenho essa confirmação no contexto oficial. Posso encaminhar para análise.
```

Justificativa: o prompt reduz o papel do modelo para uma tarefa estreita, declara a fonte de verdade, inclui fallback explícito, limita alucinação de normas externas e usa exemplos focados no erro observado. Eu não pediria para o modelo expor chain-of-thought; pediria apenas verificação interna e resposta final objetiva. Também mantive o texto deliberadamente curto para ficar com margem abaixo do limite de 300 tokens.

### 2c — Medição de impacto na qualidade

Eu mediria qualidade antes do rollout com um dataset dourado de perguntas reais e sintéticas sobre contestação, incluindo variações como "6 meses", "180 dias", "dois meses", "61 dias" e "chargeback". A métrica principal seria factualidade: a resposta citou exatamente a política oficial e não inventou prazo, valor ou regulamentação.

Métricas de negócio e qualidade:

- Taxa de alucinação em políticas de prazo/valor.
- Taxa de respostas com base oficial recuperada.
- Taxa de fallback para atendimento humano.
- Resolução no primeiro contato.
- Escalonamento humano por intenção.
- CSAT ou avaliação pós-atendimento.
- Reabertura de conversa sobre o mesmo tema.
- Tempo médio de conversa e número de turnos até resolução.

Métricas técnicas em CloudWatch ou Datadog:

- Tokens de entrada, tokens de saída e custo por sessão.
- Distribuição p50/p95/p99 de tokens por intenção.
- Latência p50/p95/p99 da chamada ao modelo.
- Erros, throttling e timeouts do provedor de inferência.
- `retrieval_hit_rate`, `retrieval_empty_rate` e documentos recuperados por versão.
- Taxa de bloqueio do guardrail.
- `prompt_version` versus taxa de sucesso.
- Cache hit rate de embeddings e resultados de retrieval.

O rollout deve ser gradual: primeiro replay offline com logs históricos anonimizados, depois shadow test sem impactar clientes, depois canary de 5% a 10% do tráfego. A mudança só deve avançar se reduzir custo sem aumentar alucinação, escalonamento ou queda de CSAT.

### IA utilizada

Ferramentas usadas: ChatGPT e Codex (OpenAI).

Como usei: utilizei como apoio para revisar a estrutura da resposta, comparar alternativas de redução de contexto e melhorar a clareza da justificativa.

O que eu modifiquei/complementei manualmente: defini o orçamento de tokens, priorizei RAG seletivo, sliding window, sumarização factual, token budget e cache seguro de embeddings/retrieval. Também removi abordagens genéricas ou pouco aderentes ao enunciado, como cache indiscriminado de resposta final personalizada, e pedi validação ativa do tamanho do system prompt para mantê-lo dentro do limite pedido.

Validação manual: revisei os trade-offs para manter a solução aderente ao problema do enunciado: custo por sessão crescendo por excesso de contexto, sem degradar qualidade em política financeira. Como o enunciado não fixa o tokenizer do provedor, conferi o prompt por contagem conservadora de palavras e caracteres: ele ficou com 125 palavras e 881 caracteres, mantendo margem abaixo de 300 tokens. Na integração real, eu travaria esse limite no CI com o tokenizer exato do modelo escolhido.

## Exercício 3

### 3a — Causas técnicas possíveis para a inconsistência

Pelo sintoma, a Resposta B aparece em parte das sessões porque o modelo não está suficientemente condicionado a uma fonte de verdade. Possíveis causas:

- Temperatura ou `top_p` altos, aumentando variação em pergunta que deveria ser determinística.
- System prompt longo e pouco específico, deixando a regra de 60 dias com baixa saliência.
- Histórico completo contaminando a resposta, por exemplo cliente ou atendente mencionando "180 dias" em turnos anteriores.
- Base de conhecimento com documentos antigos, conflitantes ou chunks que misturam contestação interna, chargeback, reembolso e regulamentação externa.
- Retrieval trazendo documento errado ou nenhum documento, mas o prompt não obriga fallback.
- Prompt sem instrução explícita para não citar regulamentação externa quando ela não estiver na base oficial.
- Ausência de validação pós-resposta para prazos, valores e entidades regulatórias.
- Mudança de versão do modelo ou parâmetros sem avaliação regressionável.
- Falta de logs de `prompt_version`, documentos recuperados e parâmetros por resposta, dificultando rastrear a causa raiz.

### 3b — Plano de teste A/B para encontrar causa raiz

Eu começaria com teste offline, porque é mais barato, controlado e seguro. Usaria uma base fixa com perguntas reais e variações sintéticas, mantendo o mesmo modelo e a mesma versão da base. Depois rodaria canary online.

Grupos de teste:

| Grupo | Variável alterada | Objetivo |
| --- | --- | --- |
| Controle | Prompt e parâmetros atuais | Medir baseline de ~15% de alucinação |
| A1 | `temperature = 0`, mantendo `top_p` original | Isolar impacto da temperatura na variância da resposta |
| A2 | `top_p` reduzido, mantendo temperatura original | Isolar impacto da amostragem por núcleo |
| B | System prompt restritivo com fallback | Verificar se falta instrução de grounding |
| C | RAG apenas com política oficial versionada | Verificar se contexto atual está contaminado |
| D | Histórico reduzido com sliding window | Verificar se histórico completo induz erro |
| E | Prompt restritivo + RAG oficial + histórico reduzido + parâmetros determinísticos | Medir solução candidata de produção |

A métrica primária seria taxa de respostas com prazo incorreto ou sem evidência oficial. Métricas secundárias: taxa de fallback, satisfação, latência, tokens e escalonamento humano. Para cada resposta eu registraria `model_id`, `temperature`, `top_p`, `prompt_version`, `context_tokens`, `retrieved_doc_ids`, `policy_version` e classificação automática/humana da resposta.

Eu classificaria cada resposta do dataset dourado em uma matriz simples:

- correta com evidência oficial;
- correta, mas sem evidência recuperada;
- incorreta por prazo, valor ou regra;
- citação externa não suportada;
- fallback adequado;
- fallback desnecessário.

No online A/B, eu manteria o controle com a versão atual e colocaria a candidata em 5% a 10% do tráfego de contestação. O teste deve ser estratificado por intenção para não comparar tráfegos diferentes. Critério de aprovação: reduzir significativamente a taxa de alucinação, idealmente para abaixo de 1%, sem piorar CSAT, fallback e resolução no primeiro contato.

### 3c — Guardrail para bloquear prazos ou valores não oficiais

Eu não deixaria essa validação apenas para outro modelo, porque prazos e valores são fatos estruturados. A solução mais segura é uma camada determinística com apoio opcional de LLM judge.

Arquitetura proposta:

```text
Resposta candidata do modelo
        |
        v
Lambda Guardrail
  1. Extrai claims sensíveis: prazos, valores, percentuais, nomes de reguladores.
  2. Consulta Policy Facts versionado: dispute_window_days=60, policy_version=...
  3. Verifica se cada claim está presente no contexto oficial recuperado.
  4. Se estiver suportado, libera.
  5. Se não estiver suportado, bloqueia e aciona regeneração ou fallback seguro.
        |
        v
Resposta final ao cliente ou escalonamento humano
```

Exemplo de regra:

```pseudo
claims = extract_numbers_and_units(answer)

for claim in claims:
    if claim.type in ["days", "months", "currency", "percentage"]:
        if not official_policy.contains(claim):
            block_answer(reason="unsupported_business_fact", claim=claim)

if answer mentions ["Banco Central", "regulamentação", "lei"] and official_context has no such source:
    block_answer(reason="unsupported_external_authority")
```

A extração pode combinar regex e NER:

- Prazos: `\b\d+\s*(dia|dias|mês|meses|ano|anos)\b`
- Valores: `R\$\s?\d+`, percentuais e limites operacionais.
- Entidades sensíveis: Banco Central, Visa, Mastercard, Procon, lei, resolução.

Se bloquear, eu usaria uma destas ações:

- Regenerar a resposta com uma mensagem de correção: "A resposta anterior citou um prazo não suportado. Responda usando apenas 60 dias ou fallback."
- Retornar template seguro para casos determinísticos.
- Escalonar para humano quando a política oficial não cobrir o caso.

Também usaria logs e métricas para medir `guardrail_block_rate`, claims bloqueadas por tipo e impacto na resolução. Um segundo modelo pode atuar como avaliador semântico, mas a decisão sobre prazo e valor deve depender da base oficial estruturada.

### IA utilizada

Ferramentas usadas: ChatGPT e Codex (OpenAI).

Como usei: utilizei como apoio para organizar as causas técnicas, estruturar o plano de teste A/B e revisar a proposta de guardrail.

O que eu modifiquei/complementei manualmente: separei `temperature` e `top_p` em grupos diferentes para isolar variáveis, adicionei matriz de classificação das respostas e defini um guardrail determinístico com Lambda, extração de claims e Policy Facts versionado.

Validação manual: mantive a decisão de não depender apenas de outro modelo para validar prazos e valores, porque fatos financeiros estruturados devem ser conferidos contra a base oficial.

## Exercício 4

### 4a — Arquitetura N8n + Lambda e responsabilidades

Interpretação adotada: N8n é o orquestrador do fluxo, mas não deve manipular transcript em plaintext. A Lambda concentra lógica sensível, acesso a segredo, criptografia, integrações externas e idempotência.

Fluxo proposto:

```text
Chatbot / Session Service
  - Encerra sessão
  - Detecta sentimento negativo
  - Salva transcript original criptografado em S3 raw-transcripts
  - Publica evento SessionEnded em SQS com event_id, session_id, sentiment e transcript_ref

N8n
  - SQS Trigger: recebe evento sem transcript em plaintext
  - Validation Node: valida event_id, session_id, customer_id, sentiment e transcript_ref
  - IF Node: se sentiment != negative, encerra workflow
  - Lambda/HTTP Node: se sentiment == negative, chama Lambda negative-session-handler
  - Status/Error Node: registra sucesso ou falha operacional

Lambda negative-session-handler
  - Garante idempotência por event_id
  - Lê/decripta transcript original em raw-transcripts usando IAM + KMS
  - Cria ticket no CRM via API REST
  - Publica alerta no SNS
  - Persiste transcript processado em audit-transcripts com metadados estruturados
  - Retorna ticket_id, sns_message_id e audit_s3_key para o N8n
```

Escolhi SQS como fluxo principal porque ela fornece buffer, retry, `VisibilityTimeout`, `maxReceiveCount` e DLQ de forma nativa. EventBridge ou webhook direto seriam alternativas possíveis, mas SQS reduz acoplamento entre o serviço de sessão e o N8n.

Separação dos objetos em S3:

- `raw-transcripts`: transcript original criptografado, salvo antes do N8n e referenciado por `transcript_ref`.
- `audit-transcripts`: transcript processado ou cópia auditável com metadados estruturados, `ticket_id`, `sns_message_id`, status do processamento, timestamps e referência ao objeto original.

Pseudocódigo do workflow:

```pseudo
on SessionEnded(event):
    assert event.event_id
    assert event.session_id
    assert event.sentiment

    if event.sentiment.label != "negative":
        return "ignored"

    result = invoke_lambda("negative-session-handler", {
        event_id: event.event_id,
        session_id: event.session_id,
        customer_id: event.customer_id,
        sentiment: event.sentiment,
        transcript_ref: event.transcript_ref
    })

    if result.status == "processed":
        mark_workflow_success(result.ticket_id, result.audit_s3_key)
    else:
        mark_workflow_failure(event.event_id, result.reason)
        fail_workflow_to_allow_sqs_retry(event)
```

Pseudocódigo da Lambda:

```pseudo
handle(event):
    previous = idempotency_store.get(event.event_id)
    if previous.status == "processed":
        return previous.result

    idempotency_store.start_or_resume(event.event_id)

    transcript = transcript_store.decrypt_and_read(event.transcript_ref)

    ticket = crm.create_ticket(
        idempotency_key=event.event_id,
        customer_id=event.customer_id,
        session_id=event.session_id,
        sentiment=event.sentiment,
        summary=build_safe_summary(transcript)
    )
    idempotency_store.mark_step_done("crm", ticket.id)

    sns_message = sns.publish(
        topic="risk-alerts",
        message={
            event_id: event.event_id,
            session_id: event.session_id,
            customer_id: event.customer_id,
            ticket_id: ticket.id,
            sentiment: event.sentiment
        }
    )
    idempotency_store.mark_step_done("sns", sns_message.id)

    audit_s3_key = s3.put_object(
        bucket="audit-transcripts",
        key=f"sessions/{event.session_id}/{event.event_id}.json",
        body=transcript,
        sse_kms_key_id=KMS_KEY_ID,
        metadata={
            "event_id": event.event_id,
            "session_id": event.session_id,
            "ticket_id": ticket.id,
            "sns_message_id": sns_message.id,
            "sentiment": event.sentiment.label,
            "policy_version": event.policy_version,
            "raw_transcript_ref": event.transcript_ref
        }
    )
    idempotency_store.mark_step_done("s3", audit_s3_key)

    result = { "status": "processed", "ticket_id": ticket.id, "audit_s3_key": audit_s3_key }
    idempotency_store.finish(event.event_id, result)
    return result
```

Responsabilidades:

- SQS: entrada resiliente do fluxo, buffer, retry primário e DLQ.
- N8n: orquestração, roteamento por sentimento, validação leve e visibilidade do workflow.
- Lambda: regras de negócio, idempotência, chamadas ao CRM, publicação SNS, escrita S3, descriptografia e acesso a segredos.
- DynamoDB ou PostgreSQL: estado de idempotência por `event_id` e status por etapa.
- Secrets Manager: credenciais do CRM.
- KMS: criptografia/decriptografia do transcript.
- S3: `raw-transcripts` para transcript original criptografado e `audit-transcripts` para transcript processado com metadados.
- SNS: alerta para o time de risco.

### 4b — Pontos de falha e resiliência

Principais falhas:

- Evento duplicado de sessão encerrada.
- Timeout ou indisponibilidade da API do CRM.
- Ticket criado no CRM, mas resposta perdida por timeout.
- Falha de publish no SNS.
- Falha ao gravar transcript no S3.
- Lambda ou N8n excedendo timeout.
- Transcript ausente, corrompido ou sem permissão KMS.
- Rate limit em CRM, SNS ou S3.

Mitigações:

- Idempotência por `event_id` em todas as etapas.
- Chave de idempotência também enviada ao CRM, se a API suportar.
- Estado por etapa em DynamoDB: `crm_done`, `sns_done`, `s3_done`. Em retry, executar somente o que falta.
- Retry primário na SQS com `VisibilityTimeout`, `maxReceiveCount` e DLQ.
- Retry curto no N8n apenas para falhas transitórias ao chamar a Lambda; falhas persistentes devem fazer o evento voltar para a fila ou seguir para DLQ.
- Retries internos na Lambda com exponential backoff e jitter para dependências como CRM e SNS.
- Timeout explícito e circuit breaker para CRM.
- Alarmes para falhas por etapa, idade da DLQ e taxa de retries.
- S3 key determinística por `session_id`/`event_id`, evitando múltiplos arquivos para o mesmo evento.
- Logs estruturados apenas com identificadores técnicos, como `event_id`, `session_id`, `ticket_id`, `s3_key`, `workflow_execution_id`, etapa e código de erro. Transcript completo e PII devem ser omitidos ou mascarados.

Se a consistência entre CRM, SNS e S3 fosse absolutamente crítica, eu documentaria esse risco e manteria a Lambda persistindo progresso por etapa para suportar retry seguro, sem tirar o N8n do papel de orquestrador pedido no enunciado.

### 4c — Transcript sem plaintext no N8n

A forma mais segura é o N8n nunca receber o transcript aberto. O serviço de sessão deve salvar o transcript criptografado em `raw-transcripts` antes do workflow, e o evento enviado pela SQS ao N8n deve conter apenas `transcript_ref`, `session_id`, `customer_id`, sentimento e metadados mínimos.

Controles técnicos:

- Transcript armazenado em S3 com SSE-KMS ou criptografia client-side com envelope encryption.
- Bucket policy permitindo leitura/decriptografia apenas para a role da Lambda.
- Role do N8n sem permissão de `kms:Decrypt` e sem acesso ao objeto bruto.
- N8n manipula somente referência opaca, por exemplo `s3://bucket/key` ou `transcript_token`.
- Lambda lê e decripta o transcript em memória, usa Secrets Manager para CRM e grava o resultado final em `audit-transcripts` com SSE-KMS.
- Logs do N8n e da Lambda devem mascarar PII e nunca registrar transcript completo.
- Para envio ao CRM, preferir resumo seguro ou campos necessários, não transcript completo, salvo requisito explícito de negócio.

Se por alguma restrição o transcript tiver que passar pelo N8n, ele deve trafegar como ciphertext envelope-encrypted, e o N8n deve apenas repassar o blob sem chave de decriptação. Ainda assim, a opção por referência segura é melhor operacionalmente e reduz risco de vazamento em logs do workflow.

### Testes e validação

- Evento com sentimento positivo não chama Lambda.
- Evento negativo chama Lambda com `transcript_ref`, sem transcript plaintext.
- Duplicidade de `event_id` retorna resultado anterior e não cria novo ticket.
- Falha temporária no CRM gera retry sem duplicar ticket.
- Falha no SNS executa retry apenas do publish, sem recriar ticket.
- Falha no S3 executa retry apenas da gravação do transcript.
- Evento inválido ou com falha persistente vai para DLQ com motivo de validação/processamento.
- N8n e Lambda não registram transcript em logs.
- Role do N8n não consegue decriptar transcript via KMS.

### IA utilizada

Ferramentas usadas: ChatGPT e Codex (OpenAI).

Como usei: utilizei como apoio para organizar a arquitetura N8n + Lambda, mapear falhas e revisar a estratégia de segurança do transcript.

O que eu modifiquei/complementei manualmente: escolhi SQS como fluxo principal, separei `raw-transcripts` e `audit-transcripts`, defini retry primário via SQS/DLQ e mantive o N8n como orquestrador sem acesso ao transcript em plaintext.

Validação manual: revisei a divisão de responsabilidades para garantir que a Lambda concentre lógica sensível, idempotência, acesso a KMS/Secrets e integrações com CRM, SNS e S3.
