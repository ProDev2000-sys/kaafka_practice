using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ProducerMicroService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ConsumerController : ControllerBase
    {
        // GET: api/<ConsumerController>
        
     
             private ConsumerConfig _config;
        public ConsumerController(ConsumerConfig config)
        {
            this._config = config;
        }

        [HttpGet]
        public string Get()
        {
            using (var consumer = new ConsumerBuilder<Null, string>(_config).Build())
            {

                 consumer.Subscribe("demo");


                var cr = consumer.Consume();
               
                Console.WriteLine(cr.Message.Value);

                return cr.Message.Value;
                

            }

        }

       

           
        

       
    }
}

