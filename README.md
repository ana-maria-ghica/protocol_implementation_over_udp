GHICA ANA-MARIA 324CD

DESCRIEREA IMPLEMENTARII:

**Functiile din fisierul [libsend.c]**
setup_connection()
    - Creez o conexiune noua, o initializez (folosind un conn_cnt = contor global), setand campul servaddr cu valorile primite ca parametru (ip, port)
    - Implementez "three-way-handshake":
        -pentru partea de inceput a conexiunii, am ales sa folosesc segmente care sunt implementate cu header-ul: "poli_tcp_ctrl_hdr"
        -astfel, definesc: "syn", "syn_ack", "ack", corespunzatoare celor 3 etape, diferentierea intre ele se face prin campul TYPE, care are valorile 100, 101, 102 mapate pentru fiecare tip.

        -clientul trimite segmentul "syn", care contine seq_num setat pe 0
        -serverul primeste segmentul "syn", si trimite inapoi catre client un segment care va fi interpretat drept "syn_ack", care va avea ack-ul = syn.seq_num+1 (exact ca in teoria implementarii)
        -clientul verifica daca ack-ul primit este valid, si defineste si trimite apoi segmentul "ack", pentru care va seta campul ack=syn_ack.seq_num+1

init_sender()
    - Fac conversia unitatilor de masura pentru a transforma din (Mb => bytes) si din (s => ms)
    - Calculez bandwidth_delay_product = speed x delay