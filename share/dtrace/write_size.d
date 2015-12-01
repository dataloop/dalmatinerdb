#!/usr/sbin/dtrace -s
/*
 * arg0 arg1 arg2 arg3 arg4  arg5 arg6   arg7 arg8
 * PID       ID   Time Size       Module Key  Operation
 *
 * PID -> Erlang process ID. (String)
 * ID -> ID of the mesurement group, 801 for cowboy handler calls.
 * Type -> Indicates of this is a entry (1) or return (2).
 * Found -> 1 if the item was found 2 if not
 * Module - The module that was called. (string).
 *
 * run with: dtrace -s reads.d
 */

/*
 * This function gets called every time a erlang developer probe is
 * fired, we filter for 801 and 1, so it gets executed when a handler
 * function is entered.
 */

erlang$1:::user_trace*
/ arg2 == 4411 /
{
  /*
   * We cache the relevant strings
   */
  @[arg4] = sum(arg4);
}


tick-1s
{
  printa(@);
}
