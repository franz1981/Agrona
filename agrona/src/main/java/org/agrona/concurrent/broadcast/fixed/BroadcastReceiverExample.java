package org.agrona.concurrent.broadcast.fixed;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.UnsafeBuffer;

public class BroadcastReceiverExample {
   private static final String FILE_NAME = "/dev/shm/broadcast";
   private static final int MSG_CAPACITY = 1024;
   private static final int MAX_MESSAGE_SIZE = Long.BYTES;
   private static final int CAPACITY = org.agrona.concurrent.broadcast.fixed.BroadcastBufferDescriptor.calculateCapacity(MAX_MESSAGE_SIZE, MSG_CAPACITY);
   private static final int TOTAL_BUFFER_LENGTH = CAPACITY + BroadcastBufferDescriptor.TRAILER_LENGTH;
   private static final AtomicBoolean STOP = new AtomicBoolean(false);

   public static void main(String[] args) throws IOException {
      long readMessages = 0;
      try (final FileChannel file = FileChannel.open(Paths.get(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
         final UnsafeBuffer buffer = new UnsafeBuffer(file.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_BUFFER_LENGTH));
         // a producer must be started already to set the record size -> this can be changed!
         BroadcastReceiver rawReceiver = new BroadcastReceiver(buffer, true);
         CopyBroadcastReceiver receiver = new CopyBroadcastReceiver(rawReceiver, false);
         while (!STOP.get()) {
            readMessages += receiver.receive((msgTypeId, receivedBuffer, index, length) -> {
               if (msgTypeId == 1) {
                  assert length == Long.BYTES;
                  System.out.println("received msgType = " + msgTypeId + " " + receivedBuffer.getLong(index));
               } else {
                  STOP.lazySet(true);
               }
            }, Integer.MAX_VALUE);
         } System.out.println("received " + readMessages + " lost " + rawReceiver.lostTransmissions());
      }
   }
}
