using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using WorkerKafkaQuestao.Data;
using WorkerKafkaQuestao.Kafka;

namespace WorkerKafkaQuestao
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly VotacaoRepository _repository;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public Worker(ILogger<Worker> logger, IConfiguration configuration,
            VotacaoRepository repository)
        {
            _logger = logger;
            _configuration = configuration;
            _repository = repository;
            _consumer = KafkaExtensions.CreateConsumer(configuration);
            _jsonSerializerOptions = new JsonSerializerOptions()
            {
                PropertyNameCaseInsensitive = true
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            string topico = _configuration["ApacheKafka:Topic"];

            _logger.LogInformation($"Topic = {topico}");
            _logger.LogInformation($"Group Id = {_configuration["ApacheKafka:GroupId"]}");
            _logger.LogInformation("Aguardando mensagens...");
            _consumer.Subscribe(topico);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    var result = _consumer.Consume(stoppingToken);
                    var eventData = result.Message.Value;
                    
                    _logger.LogInformation(
                        $"[{_configuration["ApacheKafka:GroupId"]} | Nova mensagem] " +
                        eventData);

                    ProcessEvent(eventData);
                });
           }           
        }

        private void ProcessEvent(string eventData)
        {
            _logger.LogInformation($"[{DateTime.Now:HH:mm:ss} Evento] " + eventData);

            QuestaoEventData questaoEventData = null;
            try
            {
                questaoEventData = JsonSerializer.Deserialize<QuestaoEventData>(
                    eventData, _jsonSerializerOptions);
            }
            catch
            {
                _logger.LogError(
                    "Erro durante a deserializacao dos dados recebidos!");
            }

            if (questaoEventData is not null)
            {
                if (!String.IsNullOrWhiteSpace(questaoEventData.IdVoto) &
                    !String.IsNullOrWhiteSpace(questaoEventData.Horario))
                {
                    if (!String.IsNullOrWhiteSpace(questaoEventData.Tecnologia))
                    {
                        _repository.SaveVotoTecnologia(questaoEventData);
                        _logger.LogInformation($"Voto = {questaoEventData.IdVoto} | " +
                            $"Tecnologia = {questaoEventData.Tecnologia} | " +
                            "Evento computado com sucesso!");
                        return;
                    }
                    else if (!String.IsNullOrWhiteSpace(questaoEventData.Instancia))
                    {
                        _repository.SaveHistoricoProcessamento(questaoEventData);
                        _logger.LogInformation($"Voto = {questaoEventData.IdVoto} | " +
                            $"Instância = {questaoEventData.Instancia} | " +
                            "Evento computado com sucesso!");
                        return;
                    }
                }

                _logger.LogError($"Formato dos dados do evento inválido!");
            }
        }
    }
}