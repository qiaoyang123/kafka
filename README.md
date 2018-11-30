# 此工程是从kafka消息队列读取消息，然后分发写入到不同的目的地
# consumer-api : DataWriteService 是一个统一的写入接口，不同的写入，不同的实现方式。
# consumer-client: 从kafka中读取消息，实现写入到不同的目的地的业务工程
# input-client: 封装各种写入目的地的SDK，提供基本的方法，供consumer-client调用。
