namespace java com.tinkerpop.thrift.test

enum OperationType {
  ADD,
  MUL,
  DIV,
  SUB
}

enum ArgType {
  INT,
  LONG,
}

struct Request {
  1: required i32 id;
  2: required binary arg1;
  3: required binary arg2;
  4: required ArgType argType;
  5: required OperationType operationType;
}

struct Response {
  1: i32 id;
  2: binary result;
  3: ArgType resType;
}

service TestService {
  Response invoke(1: Request req);
  oneway void ping();
}