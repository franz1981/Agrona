package org.agrona.concurrent.broadcast.fixed;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.LockSupport;

import org.agrona.concurrent.UnsafeBuffer;

public class BroadcastProducerExample {

   private static final String FILE_NAME = "/dev/shm/broadcast";
   private static final int MSG_CAPACITY = 1024;
   private static final int MAX_MESSAGE_SIZE = Long.BYTES;
   private static final int CAPACITY = org.agrona.concurrent.broadcast.fixed.BroadcastBufferDescriptor.calculateCapacity(MAX_MESSAGE_SIZE, MSG_CAPACITY);
   private static final int TOTAL_BUFFER_LENGTH = CAPACITY + BroadcastBufferDescriptor.TRAILER_LENGTH;

   public static void main(String[] args) throws IOException {
      final long MSGS = 10_000;
      System.out.println("press key to stop");
      try (final FileChannel file = FileChannel.open(Paths.get(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
         final UnsafeBuffer buffer = new UnsafeBuffer(file.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_BUFFER_LENGTH));
         BroadcastTransmitter transmitter = new BroadcastTransmitter(buffer, MAX_MESSAGE_SIZE);
         for (long i = 0; i < MSGS; i++) {
            LockSupport.parkNanos(100);
            try (BroadcastTransmitter.Transmission transmission = transmitter.transmit(1, Long.BYTES)) {
               transmission.putLong(0, i + 1);
            }
            System.out.println("transmitted " + i);
         }
         try (BroadcastTransmitter.Transmission transmission = transmitter.transmit(2, 0)) {
         }
      }
   }

}
