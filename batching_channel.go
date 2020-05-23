package channels

// BatchingChannel implements the Channel interface, with the change that instead of producing individual elements
// on Out(), it batches together the entire internal buffer each time. Trying to construct an unbuffered batching channel
// will panic, that configuration is not supported (and provides no benefit over an unbuffered NativeChannel).
type BatchingChannel struct {
	input, output chan interface{}
	length        chan int
	buffer        []interface{}
	size          BufferCap
}

func NewBatchingChannel(size BufferCap) *BatchingChannel {
	if size == None {
		panic("channels: BatchingChannel does not support unbuffered behaviour")
	}
	if size < 0 && size != Infinity {
		panic("channels: invalid negative size in NewBatchingChannel")
	}
	ch := &BatchingChannel{
		input:  make(chan interface{}),
		output: make(chan interface{}),
		length: make(chan int),
		size:   size,
	}
	go ch.batchingBuffer()
	return ch
}

// 返回可写的buffer
func (ch *BatchingChannel) In() chan<- interface{} {
	return ch.input
}

// Out returns a <-chan interface{} in order that BatchingChannel conforms to the standard Channel interface provided
// by this package, however each output value is guaranteed to be of type []interface{} - a slice collecting the most
// recent batch of values sent on the In channel. The slice is guaranteed to not be empty or nil. In practice the net
// result is that you need an additional type assertion to access the underlying values.
func (ch *BatchingChannel) Out() <-chan interface{} {
	return ch.output
}

func (ch *BatchingChannel) Len() int {
	return <-ch.length
}

func (ch *BatchingChannel) Cap() BufferCap {
	return ch.size
}

func (ch *BatchingChannel) Close() {
	close(ch.input)
}

func (ch *BatchingChannel) batchingBuffer() {
	var input, output, nextInput chan interface{}
    // channel是引用类型
	nextInput = ch.input
	input = nextInput

    // 核心的input和output
	for input != nil || output != nil {
		select {
            // open表示器是否已经close了
		case elem, open := <-input:
			if open {
                // 写进来放到buffer中
				ch.buffer = append(ch.buffer, elem)
			} else { // 写端关闭
				input = nil
				nextInput = nil
			}
		case output <- ch.buffer: // 刚开始是nil，那就是是阻塞的
            // 读完一个就置为nil?
			ch.buffer = nil
		case ch.length <- len(ch.buffer):
		}

        // 读完了
		if len(ch.buffer) == 0 {
			input = nextInput
            // 读端再次设置为nil, 为阻塞
			output = nil
		} else if ch.size != Infinity && len(ch.buffer) >= int(ch.size) { // 至多写size个数据
			input = nil  // 设置写端为阻塞的
			output = ch.output // 然后开始读
		} else {  // 其他情况，即可写，又可读
			input = nextInput
			output = ch.output
		}
	}

    // 关闭读和length channel
	close(ch.output)
	close(ch.length)
}
