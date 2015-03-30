package net;

import java.nio.ByteBuffer;

public interface VersionedProtocol {

	public static final byte CURRENT_VERSION = 3;
	public static final ByteBuffer HEADER = ByteBuffer.wrap("repl".getBytes());
}
