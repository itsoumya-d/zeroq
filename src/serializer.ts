import { Message } from './types';

export class Serializer {
  static serialize(msg: Message): string {
    return JSON.stringify(msg);
  }

  static deserialize(data: string): Message {
    return JSON.parse(data);
  }
}
