package maki.controller;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

import maki.commons.utils.RtnUtil;
import maki.models.mq.WritingDataProcesser;
import maki.service.AmqpWritingDataService;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/test")
public class TestController extends BaseController{

	@Resource(name="amqpWritingDataService")
	private AmqpWritingDataService amqpWritingDataService;
	
	@RequestMapping(value="testRabbitSend")
	public void testRabbitSend(HttpServletResponse response) throws Exception{
		WritingDataProcesser message = new WritingDataProcesser();
		message.setUserId(1);
		message.setMsgId(2);
		amqpWritingDataService.writeData2Mq(message);
		responseJson(response, RtnUtil.getOkDataRtn("ok"));
	}
}
