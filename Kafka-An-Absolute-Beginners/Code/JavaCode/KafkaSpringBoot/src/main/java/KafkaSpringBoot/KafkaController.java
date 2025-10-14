package KafkaSpringBoot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/send")
    public String sendMessage(@RequestBody StudentDetails user) {
        kafkaProducerService.sendMessage(user);
        return "Message sent to Kafka: " + user.getName();
    }
}
