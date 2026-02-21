import threading, signal
from libacqingv import ParosReader, DatalinkWriter, Packet_Handler

PAROS_HOST     = "10.212.20.4"
PAROS_PORT1    = 4001
PAROS_PORT2    = 4001
PAROS_REGEX    = r"\*(?P<datetime>\d{2}/\d{2}/\d{2}T\d{2}:\d{2}:\d{2}),(?P<sample>\d{3}\.\d{4})\r" #\r\n
PAROS_RAW      = r"\*(?P<sample>\d{3}\.\d{4})\r"

DATALINK_HOST  = "10.201.4.11"
DATALINK_PORT  = 16000

STATION        = "IT.BOA.01.LDR"


def main():

    shutdown_event = threading.Event()


    def handle_signal(signum, frame):
    
        print(f"\n[MAIN] Interruzione ricevuta (sig: {signum}). Arresto dei thread in corso...")
    
        acq_boa1.terminate(10)
    
        acq_boa2.terminate(10)
    
        ph.terminate()

        send_mssed.terminate(10)
    
        #ph.terminate()
    
        shutdown_event.set()


    signal.signal(signal.SIGINT, handle_signal)
    
    signal.signal(signal.SIGTERM, handle_signal)


    try:
        ph = Packet_Handler(
            regex = PAROS_REGEX,
            station = STATION
        )
   
        acq_boa1    = ParosReader(
            name    = "PAROS_Thread_1",
            host    = PAROS_HOST,
            port    = PAROS_PORT1,
            regex   = PAROS_REGEX,
            on_data = ph.put
        )
        
        acq_boa2     = ParosReader(
            name    = "PAROS_Thread_2",
            host    = PAROS_HOST,
            port    = PAROS_PORT2,
            regex   = PAROS_REGEX,
            on_data = ph.put
        )

        send_mssed = DatalinkWriter(
            name    = "Datalink_Thread",
            host    = DATALINK_HOST,
            port    = DATALINK_PORT,
            queue   = ph   #get dalla coda 
        )

        acq_boa1.start()

        acq_boa2.start()

        send_mssed.start()

        while not shutdown_event.is_set():
            shutdown_event.wait(1)
    
    except Exception as e:
        print(f"[MAIN] Exception: {e}")
    
    finally:
        print("[MAIN] Programma terminato.")

if __name__ == "__main__":
    main()
