import os, socket, re, time, threading, asyncio, collections
from types import MethodType
import numpy as np
from simpledali import SocketDataLink
from simplemseed import encode, MSeed3Header, MSeed3Record, FDSNSourceId, NetworkSourceId, MiniseedHeader, MiniseedRecord
from abc import ABC, abstractmethod
from typing import Generator, Any
from queue import Queue, Empty, ShutDown
from datetime import datetime, timedelta


logfile = r"log-itboa.txt"

class Packet_Handler():
    """
    La classe Packet_Handler gestisce la coda dei pacchetti acquisiti dalla classe 
    thread ParosReader e vengono successivamente prevelati dalla classe thread DatalinkWriter 
    che si occupa dell'invio al server ringserver mediante protocollo datalink 
    
    Si è scelto di creare una classe iterabile per rendere il codice più leggibile.
    Durante l'iterazione viene estratto dalla coda il pacchetto acquisito e viene convertito
    in miniseed versione 2
    Sono implementati entarmbi i pacchetti miniseed versione 2 e 3, di default viene 
    utilizzata la versione 2.
    """

    __QUEUESIZE: int = 86400

    __queue: Queue = None

    __cache: collections.OrderedDict = None

    __running: bool = True

    __TIMEOUT: int = 5


    def __init__(self, regex: str = None, station: str = None):

        self.__name = self.__class__.__name__

        self.__queue = Queue()

        self.__queue.maxsize = self.__QUEUESIZE

        self.__cache = collections.OrderedDict()

        self.__regex = re.compile(regex)

        self.__network, self.__station, self.__location, self.__channel = station.split(".")
    
        self.__last_get = None

        self.__gap_count = 0
 
        self.__gap_seconds = 0

        self.__delta = timedelta(seconds = 30)

        self.__last_time = None #datetime.now() - timedelta(seconds = self.__QUEUESIZE)

        self.__last_is_except = False


    def __iter__(self):
 
        return self


    def __next__(self):

        try:

            if not self.__running: raise StopIteration

            # prendi dalla coda se non c'è stata un'eccezione o se l'ultimo dato non continene valori
            if not self.__last_is_except or self.__last_get is None:
                self.__last_get = self.get(timeout = self.__TIMEOUT)
            
            if self.__last_get is None: return None

            self.__last_is_except = False

            acq_thread, acq_data, acq_sample, acq_raw = self.__last_get

            if (
                self.__last_time is None 
                and 
                (datetime.now() - self.__delta < acq_data < datetime.now() + self.__delta)
            ): self.__last_time = acq_data - timedelta(seconds = 1)

            # controlla che la data dell'ultimo campione sia superiore al precedente e inferiore a now() + delta
            if (
                self.__last_time is not None
                and
                not (self.__last_time < acq_data < datetime.now() + self.__delta)
            ) : return None #inserito il 31-01-2026 

            if acq_data not in self.__cache:
                
                self.__cache[acq_data] = acq_sample
                
                msrecord = self.__create_mseed(acq_data, acq_sample)
                
                diff = self.__update_statistics(acq_data)

                message = f"[{acq_thread} (Cache:{len(self.__cache)}, Queue:{self.__queue.qsize()})]: " \
                           f"{acq_data} {acq_sample} (RAW: {acq_raw.encode("utf-8")}) " \
                           f"Delta:{diff}s, Gap:({self.__gap_count}, {self.__gap_seconds}s)"              

                print(f"{message}")
                
                if diff > 1.0:
                    self.log = message              
                
                self.__last_time = acq_data

                # Creare un thread o un task asyncio per gestire la cache
                if len(self.__cache) > 60:
                    self.__cache.popitem(last=False)

                self.__last_is_except = False

                return msrecord
            
            else:
                return None

        except Empty:
            return None
        
        except ShutDown:
            raise StopIteration

        except StopIteration:
            raise StopIteration

        except Exception as e:
            #if self.__running: self.undo()
            self.__last_is_except = True
            
            self.log = f"__iter__() => Exception: {e}"
            
            return None


    def __get_valid_packet(self):

        while self.__running:

            try:
                
                acq_thread, acq_raw = self.get(timeout = self.__TIMEOUT)

                self.log = f"[{acq_thread}] => RAW: ({acq_raw})" #, sample: ({acq_sample}) RAW: ({acq_raw})\n")
                
                acq_raw = bytes(acq_raw).decode("utf-8", errors="ignore").rstrip("\r") 
                
                iters = self.__regex.finditer(acq_raw)
                
                if iters:
                    
                    return iters
                    for match in iters:
                        acq_data = datetime.strptime(match.group("datetime"), "%y/%m/%dT%H:%M:%S")
                        acq_sample = np.array([int(float(match.group("sample"))*10000)], dtype = np.int32)
                        
                        self.log = f"\t\t=> datetime: ({acq_data}), sample: ({acq_sample})"
                
                #if match:
                #    acq_data, acq_sample = match.group(0).lstrip("*").split(",")

                else:
                    raise Exception(f"Match reg expression")

                #acq_data = datetime.strptime(acq_data, "%y/%m/%dT%H:%M:%S")

                #acq_sample = np.array([int(float(acq_sample)*10000)], dtype = np.int32)

                return acq_thread, acq_data, acq_sample, acq_raw


            except Exception as e:
                self.log = f"[{acq_thread}] => RAW: ({acq_raw}), Exception: {e}"
    

    def __update_statistics(self, acq_data: datetime):

        diff = 1.0 if self.__last_time is None else (acq_data - self.__last_time).total_seconds()

        if diff > 1:
            self.__gap_count  += 1
            self.__gap_seconds += (diff-1)

        return diff


    def __create_mseed(self, acq_data: datetime, acq_sample: np.ndarray) -> MiniseedRecord | None:        

        header = MiniseedHeader(
            network = self.__network,
            station = self.__station,
            location = self.__location,
            channel = self.__channel,
            starttime = acq_data,
            numSamples= acq_sample.size,
            sampleRate = 1.0,
            sampRateFactor= int(1),
            sampRateMult= int(1)
        )

        ms2record = MiniseedRecord(
            header = header, 
            data = encode(acq_sample)
        )

        return ms2record


    def __create_mseed3(self, acq_data: str, acq_sample: str) -> MSeed3Record | None:

        header = MSeed3Header()

        header.publicationVersion = 0

        header.starttime = datetime.strptime(acq_data, f"%y/%m/%dT%H:%M:%S")

        header.sampleRatePeriod = 1.0

        header.numSamples = int(1)

        ms3record = MSeed3Record(
            header = header, 
            identifier = FDSNSourceId("IT", "BOA", "", "L", "D", "G"), 
            data = np.array([float(acq_sample)], dtype = np.float32)
        )

        #self.log = f"SendMSSED -> Before writeMSeed {ms3record.header.starttime.isoformat()} {ms3record.details()} bytes: {len(ms3record.pack())} {ms3record.header.encoding}"

        return ms3record        


    def __undo(self):

        # Inserire nella coda solo acq_thread e acq_raw
        if self.__last_get is not None:

            acq_thread, acq_data, acq_sample, acq_raw = self.__last_get

            self.__queue.put(self.__last_get)

            self.__cache.pop(acq_data)

            self.log = f"[Undo] Dato reinserito nel buffer"


    def __log(self, message: str): 

        print(f"[{self.__name}] {message}")
        with open(logfile, "a", encoding = "utf-8") as f:
            f.write(f"[{self.__name}] {message}\n")


    log = property(fset = __log)


    def get(self, block: bool= True, timeout: float | None = None):

        return self.__queue.get(block = block, timeout = timeout)


    # utilizzata dalla classe ParosReader()
    def put(self, item: Any, block: bool = True, timeout: float | None = None):
        
        #inserire e self.__TIMEOUT e gestire il caso della coda piena 
        self.__queue.put(item = item, block = block, timeout = timeout)


    def terminate(self):

        self.__running = False


class MyThreadClass(ABC):
    """
    MyThreadClass è la classe base per le due classi ParosReader() e
    DatalinkWriter() ed esegue un thread che come target richiama il metodo 
    astratto worker.
    La classe implementa logging e flag per il controllo del thread
    """

    __LOOP_TIME = 0.05
    
    __WAIT_TIME = 5


    def __init__(self, name: str):

        super().__init__()

        self.name = name

        self.__shutdown_event = False


    # Dichiarazione delle proprietà getter e setter

    @property
    def name(self) -> str:

        return self.__name


    @name.setter
    def name(self, name: str):

        if isinstance(name, str):
            self.__name = name

        else:
            self.__name = self.__class__.__name__

    @property
    def running(self) -> bool:

        return not self.__shutdown_event


    @running.setter
    def running(self, run: bool):

        self.__shutdown_event = not run


    # Dichiarazione dei metodi privati

    def __log(self, message: str):

        #print(f"[{self.__name}] {message}")
        with open(logfile, "a", encoding = "utf-8") as f:
            f.write(f"[{self.__name}] {message}\n")


    # Dichiarazione dei metodi pubblici

    log = property(fset = __log)

    
    def process_messages(self):

        time.sleep(self.__LOOP_TIME)


    def wait(self):

        time.sleep(self.__WAIT_TIME)

    
    async def async_sleep(self):

        await asyncio.sleep(self.__LOOP_TIME)


    async def async_wait(self):

        await asyncio.sleep(self.__WAIT_TIME)


    def start(self):

        self.__thread = threading.Thread(
            target = self.worker, 
            args = (), 
            name = self.__name, 
            daemon = True
        )

        self.running = True

        self.__thread.start()

        self.log = f"Start() => Thread avviato..."


    def terminate(self, timeout: float | None = None):

        self.running = False

        self.__thread.join(timeout = timeout)

        self.log = f"Terminate()=> Thread is {"alive" if self.__thread.is_alive() else "not alive"}"


    @abstractmethod
    def worker(self): pass


class ParosReader(MyThreadClass):
    """
    Classe estesa di MyThreadClass()
    
    Si connette a un socket e legge in continuo lo streaming di dati raw 
    inserendo le righe in una coda Queue() gestendo l'evento on_data 
    """

    __SOCKET_TIMEOUT = 10
    __READBUFFERSIZE = 32


    def __init__(self, host: str, port: int, name: str = None, regex: str = None, on_data: MethodType = None):

        super().__init__(name)

        self.address = (host, port)

        self.regex = regex

        self.on_data = on_data

        self.__socket = None


    # Dichiarazione delle proprietà getter e setter

    @property
    def address(self) -> tuple[str, int]:

        return (self.__host, self.__port)


    @address.setter
    def address(self, address: tuple[str, int]):

        (self.__host, self.__port) = address


    @property
    def regex(self) -> re.Pattern[str]:

        return self.__regex


    @regex.setter
    def regex(self, regex: str):

        if isinstance(regex, str):
            self.__regex = re.compile(regex)

        else:
            self.__regex = None


    @property
    def on_data(self) -> function:

        return self.__on_data


    @on_data.setter
    def on_data(self, on_data: MethodType):

        if isinstance(on_data, MethodType):
            self.__on_data = on_data

        else:
            self.__on_data = None


    # Dichiarazione dei metodi privati

    """
    def __open_socket(self) -> bool:

        try:
            self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__socket.settimeout(self.__SOCKET_TIMEOUT)
            self.__socket.connect(self.address)
            
        except socket.error as e:
            self.__socket = None
            self.log = f"OpenSocket() => Socket error: {e.strerror}"

        if self.__socket is not None:
            self.log = f"OpenSocket() => Connected to {self.__host}:{self.__port}"
            return True

        else:
            return False


    def __read_socket(self) -> bytes | None:

        #try:

        buf = self.__socket.recv(self.__READBUFFERSIZE)

        if buf is None:
            raise socket.error(f"ReadSocket() => Socket server closed: (buf is {str(buf)})")

        else:
            return buf

        #except socket.error as e:
        #    self.log = f"ReadSocket() => Socket error: {e.strerror}"
        #    return None


    def __parse_buffer(self, buffer: bytes) -> bytes:

        while b"\n" in buffer:

            try:
                line, buffer = buffer.split(b"\n", 1)

                acq_raw = line.decode("utf-8", errors="ignore") #.rstrip("\r") 
                
                iters = self.__regex.finditer(acq_raw)
                
                if iters:

                    for match in iters:
                        
                        acq_data = datetime.strptime(match.group("datetime"), "%y/%m/%dT%H:%M:%S")

                        acq_sample = np.array([int(float(match.group("sample"))*10000)], dtype = np.int32)

                        if self.on_data is not None:
                            self.on_data((self.name, acq_data, acq_sample, acq_raw))

                        else:
                            self.log = f"ExecRegEx() => Method <on_data()> not defined"

                else:
                    self.log = f"ExecRegEx() => Dont match regular expression ({line})"

            except UnicodeDecodeError as e:
                self.log = f"ExecRegEx() => Unicode decode error ({line})"

            except Exception as e:
                self.log = f"ExecRegEx() => Generic error ({line}): {e}"

        return buffer
    """
    
    async def __connect_socket(self):
        
        self.__reader, self.__writer = await asyncio.open_connection(self.__host, self.__port)

        return self.__reader is not None and self.__writer is not None


    def __aiter__(self):

        return self
    

    async def __anext__(self):
        
        if self.__reader:
            
            if self.__reader.at_eof():
            
                raise StopAsyncIteration
            
            try:
                
                line = await asyncio.wait_for(self.__reader.readline(), 5)
            
            except asyncio.TimeoutError:
                
                line = None
                
                raise StopAsyncIteration
        

        if not line or not self.running: 
            
            raise StopAsyncIteration
        

        return line or None


    async def __close_socket(self):

        if self.__writer:
            
            try:
                
                self.__writer.close()
                
                await self.__writer.wait_closed()

                self.__writer.transport.abort()
            
            except:
                
                pass


    async def socket_handler(self):
        
        while self.running:

            try:

                if await self.__connect_socket():
                    
                    async for line in self:

                        if line is not None:
                            
                            #self.log = f"[{self.name} Read Socket()] => buf: ({line})"

                            acq_raw = line.decode("utf-8", errors="ignore")

                            iters = self.__regex.finditer(acq_raw)
                            
                            if iters:

                                for match in iters:
                                    
                                    acq_data = datetime.strptime(match.group("datetime"), "%y/%m/%dT%H:%M:%S")

                                    acq_sample = np.array([int(float(match.group("sample"))*10000)], dtype = np.int32)

                                    if self.on_data is not None:
                                        self.on_data((self.name, acq_data, acq_sample, acq_raw))

                                    else:
                                        self.log = f"ExecRegEx() => Method <on_data()> not defined"

                            else:
                                self.log = f"ExecRegEx() => Dont match regular expression ({line})"

                        await self.async_sleep()

                    await self.__close_socket()
            
            except Exception as e:
                
                self.log = f"Worker => Generic error: {e}"

            finally:
            
                await self.async_wait()
    

    def worker(self):

        asyncio.run(self.socket_handler(), debug = False)

        self.log = f"Worker => Arresto del thread (running is {self.running}) in corso..."


# Dichiarazione dei metodi pubblici
    """
    def worker(self):

        while self.running:
            # modificare con socket = self.__open_socket()
            # if socket is not none: 
            if self.__open_socket():

                try:
                    buffer = b""

                    while self.running:
                        # modificare con self.__read_socket(socket)
                        buf = self.__read_socket() 
                        self.log = f"[{self.name} Read Socket()] \t=> buf: ({buffer+buf})"

                        if buf is not None:
                        
                            buffer = self.__parse_buffer(buffer + buf)                            

                        self.process_messages()

                except socket.error as e:
                    self.log = f"Worker() => Socket error: {e.strerror}\n" \
                               f"Worker() => Riconnessione tra 5 sec..."

                except Exception as e:
                    self.log = f"Worker() => Errore imprevisto: {e}"

                finally:
                    if self.__socket: self.__socket.close()

            self.wait()

        self.log = f"Worker() => Arresto del thread (running is {self.running}) in corso..."
    """

class ParosACQ(MyThreadClass):
    """
    Classe estesa di MyThreadClass()
    
    Si connette a un socket e legge in continuo lo streaming di dati raw 
    inserendo le righe in una coda Queue() gestendo l'evento on_data 
    """

    __SOCKET_TIMEOUT = 10
    __READBUFFERSIZE = 32


    def __init__(self, host: str, port: int, name: str = None, regex: str = None, on_data: MethodType = None):

        super().__init__(name)

        self.address = (host, port)

        self.regex = regex

        self.on_data = on_data

        self.__socket = None


    # Dichiarazione delle proprietà getter e setter

    @property
    def address(self) -> tuple[str, int]:

        return (self.__host, self.__port)


    @address.setter
    def address(self, address: tuple[str, int]):

        (self.__host, self.__port) = address


    @property
    def regex(self) -> re.Pattern[str]:

        return self.__regex


    @regex.setter
    def regex(self, regex: str):

        if isinstance(regex, str):
            self.__regex = re.compile(regex)

        else:
            self.__regex = None


    @property
    def on_data(self) -> function:

        return self.__on_data


    @on_data.setter
    def on_data(self, on_data: MethodType):

        if isinstance(on_data, MethodType):
            self.__on_data = on_data

        else:
            self.__on_data = None


    # Dichiarazione dei metodi privati

    async def __connect_socket(self):
        
        self.__reader, self.__writer = await asyncio.open_connection(self.__host, self.__port)

        return self.__reader is not None and self.__writer is not None


    def __aiter__(self):

        return self
    

    async def __anext__(self):
        
        if self.__reader:
            if self.__reader.at_eof():
                raise StopAsyncIteration
            
            line = await self.__reader.readline()
        
        if line is None or not self.running: 
            raise StopAsyncIteration
        
        return line or None


    async def __close_socket(self):

        if self.__writer:
            self.__writer.close()
            await self.__writer.wait_closed()


    async def socket_handler(self):
        
        if await self.__connect_socket():
            
            prev_acq_data: datetime = None

            async for line in self:

                if line is not None:
                    
                    self.log = f"[{self.name} Read Socket()] => buf: ({line})"

                    acq_raw = line.decode("utf-8", errors="ignore")

                    iters = self.__regex.finditer(acq_raw)
                    
                    if iters:

                        for match in iters:
                            
                            now_acq_data = datetime.now()

                            acq_data = now_acq_data.strftime("%y/%m/%dT%H:%M:%S.%f") #datetime.strptime(match.group("datetime"), "%y/%m/%dT%H:%M:%S")

                            acq_sample = np.array([int(float(match.group("sample"))*10000)], dtype = np.int32)

                            dt = (now_acq_data - prev_acq_data).total_seconds() if prev_acq_data is not None else 0.0

                            prev_acq_data = now_acq_data

                            f = round(1/dt,2) if dt > 0 else 0.00

                            self.log = f"[{self.name} Read Socket()] => acq: ({acq_data} {acq_sample} {f}hz ({dt}))"

                            if self.on_data is not None:
                                self.on_data((self.name, acq_data, acq_sample, acq_raw))

                            else:
                                self.log = f"ExecRegEx() => Method <on_data()> not defined"

                    else:
                        self.log = f"ExecRegEx() => Dont match regular expression ({line})"

                await self.async_sleep()
                #self.process_messages()

            await self.__close_socket()
    

    def worker(self):

        while self.running:

            try:
                asyncio.run(self.socket_handler(), debug = False)

            except Exception as e:
                self.log = f"Worker => Generic error: {e}"

            finally:
                self.process_messages()
                #self.async_sleep()

        self.log = f"Worker => Arresto del thread (running is {self.running}) in corso..."


class DatalinkWriter(MyThreadClass):
    """
    Classe estesa di MyThreadClass()
    
    Si connette a un ringserver con protocollo datalink.
    
    Scrive in continuo lo streaming di dati in miniseed2 o miniseed3 
    leggendo dalla coda Queue() i dati inseriti dal thread ParosReader() 
    """

    def __init__(self, host: str, port: int = 16000, name: str = None, queue: Packet_Handler = None):

        super().__init__(name)

        self.address = (host, port)
        
        self.__queue: Packet_Handler = queue


    # Dichiarazione dei metodi getter e setter

    @property
    def address(self) -> tuple[str, int]:

        return (self.__host, self.__port)


    @address.setter
    def address(self, values: tuple[str, int]):

        (self.__host, self.__port) = values


    # Dichiarazione dei metodi privati

    async def packet_handler(self, dlink: SocketDataLink):

        try:

            while self.running:

                for msrecord in self.__queue:

                    if msrecord is not None:

                        sendResult = await dlink.writeMSeed(msrecord)  #sendResult = await dlink.writeMSeed3(msrecord)

                        if sendResult.type == "OK": # is not None:

                            print(f"Packet => {msrecord.header.starttime.isoformat()} {msrecord.identifier} bytes: {len(msrecord.pack())} encode: {msrecord.header.encoding} packetID: {sendResult.value} result: {sendResult.type}")

                    await self.async_sleep()

        except Exception as e:
            self.log = f"Packet => Exception: {e}"


    async def socket_handler(self):

        while self.running:

            try:

                async with SocketDataLink(self.__host, self.__port, verbose = False) as dlink:
                    
                    serverId = await dlink.id("acq_boa", "ingv-coa", 0, "python")                

                    if serverId is None:
                        self.log = f"Socket Handler => No response from call id, try to reconnect..."

                    else:
                        #self.log = f"Socket Handler => Response from call id: {serverId}"
                        await self.packet_handler(dlink)

            except Exception as e:
                
                self.log = f"Worker => Generic error: {e}"

            finally:

                await self.async_wait()


    def worker(self):

        asyncio.run(self.socket_handler(), debug = False)

        self.log = f"Worker => Arresto del thread (running is {self.running}) in corso..."


