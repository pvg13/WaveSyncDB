import {Model} from '@nozbe/watermelondb';
import {field, readonly, date} from '@nozbe/watermelondb/decorators';

export class Task extends Model {
  static table = 'tasks';

  @field('title') title!: string;
  @field('done') done!: boolean;
  @readonly @date('created_at') createdAt!: Date;
}
