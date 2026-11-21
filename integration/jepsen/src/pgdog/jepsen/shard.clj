(ns pgdog.jepsen.shard
  "Key placement for sharded mode. The harness must know which shard each key
  lives on, so that every append txn touches keys on one shard only (C3) and
  the checkers can split the history per shard.

  Each shard s gets its own key sequence, (key-of test s j) for j = 0, 1, ...
  An Elle generator runs per shard over local keys j; its keys are mapped
  through key-of. --sharding picks how PgDog places them:

    :list   [[sharded_tables.mapping]] with explicit values per shard.
            Shard s owns s*base + [0, list-size).
    :range  [[sharded_tables.mapping]] with start/end per shard.
            Shard s owns [s*base, (s+1)*base).
    :hash   PgDog's default (hasher = postgres, bigint): Postgres' hash
            partitioning hash, hashint8extended with HASH_PARTITION_SEED,
            reimplemented here (see hash-bigint). Shard s owns the integers
            that hash to s, in increasing order.

  (anchor test s) is a key on shard s used to route the per-txn probe
  statements (current_setting, pg_backend_pid) to the txn's shard; see
  client/probe-sql."
  (:require [clojure.string :as str]))

(def base
  "Distance between shards' key blocks for :list and :range."
  1000000)

;; ---------------------------------------------------------------------------
;; PgDog's bigint hash (pgdog/src/frontend/router/sharding/hashfn.c,
;; sharding/mod.rs, sharding/context.rs). Arithmetic on uint32 is done in
;; longs, masked to 32 bits.

(def ^:private m32 0xFFFFFFFF)

(defn- u32 ^long [^long x] (bit-and x m32))

(defn- rot ^long [^long x ^long k]
  (u32 (bit-or (bit-shift-left x k) (unsigned-bit-shift-right (u32 x) (- 32 k)))))

(defn- mix
  "Jenkins lookup3 mix(a, b, c)."
  [a b c]
  (let [a (u32 (- a c)) a (bit-xor a (rot c 4))  c (u32 (+ c b))
        b (u32 (- b a)) b (bit-xor b (rot a 6))  a (u32 (+ a c))
        c (u32 (- c b)) c (bit-xor c (rot b 8))  b (u32 (+ b a))
        a (u32 (- a c)) a (bit-xor a (rot c 16)) c (u32 (+ c b))
        b (u32 (- b a)) b (bit-xor b (rot a 19)) a (u32 (+ a c))
        c (u32 (- c b)) c (bit-xor c (rot b 4))  b (u32 (+ b a))]
    [a b c]))

(defn- final
  "Jenkins lookup3 final(a, b, c)."
  [a b c]
  (let [c (bit-xor c b) c (u32 (- c (rot b 14)))
        a (bit-xor a c) a (u32 (- a (rot c 11)))
        b (bit-xor b a) b (u32 (- b (rot a 25)))
        c (bit-xor c b) c (u32 (- c (rot b 16)))
        a (bit-xor a c) a (u32 (- a (rot c 4)))
        b (bit-xor b a) b (u32 (- b (rot a 14)))
        c (bit-xor c b) c (u32 (- c (rot b 24)))]
    [a b c]))

(defn hash-bigint
  "hash_combine64(0, hashint8extended(k, HASH_PARTITION_SEED)) as a 64-bit
  value (treat as unsigned)."
  ^long [^long k]
  (let [lo   (u32 k)
        hi   (u32 (bit-shift-right k 32))
        lo   (u32 (bit-xor lo (if (neg? k) (u32 (bit-not hi)) hi)))
        init (u32 (+ 0x9e3779b9 4 3923095))
        ;; seed = 0x7A5B22367996DCFD: a += high half, b += low half
        [a b c] (mix (u32 (+ init 0x7A5B2236)) (u32 (+ init 0x7996DCFD)) init)
        [_ b c] (final (u32 (+ a lo)) b c)
        h    (bit-or (bit-shift-left b 32) c)]
    (unchecked-add h 0x49a0f4dd15e5a8e3)))

(defn hash-shard
  "The shard PgDog's default hasher picks for bigint k among n shards."
  ^long [^long k ^long n]
  (Long/remainderUnsigned (hash-bigint k) n))

;; ---------------------------------------------------------------------------
;; Placement

(defn shard-count [test]
  (count (:postgres (:roles test))))

(def ^:private hash-keys
  "{[n s] vector of the smallest non-negative integers that hash to s},
  grown on demand."
  (atom {}))

(defn- hash-key-of [n s j]
  (let [have (get @hash-keys [n s] [])]
    (if (< j (count have))
      (nth have j)
      (let [start (if (seq have) (inc (peek have)) 0)
            more  (->> (iterate inc start)
                       (filter #(= s (hash-shard % n)))
                       (take (max 64 (- (inc j) (count have)))))]
        (nth (get (swap! hash-keys update [n s] #(into (or % []) more)) [n s]) j)))))

(defn key-of
  "The j-th key of shard s."
  [test s j]
  (case (:sharding test)
    (:list :range)
    (do (when (and (= :list (:sharding test)) (>= j (:list-size test)))
          (throw (ex-info (str "shard " s " ran out of list-mapped keys; raise --list-size")
                          {:shard s, :j j, :list-size (:list-size test)})))
        (+ (* s base) j))

    :hash
    (hash-key-of (shard-count test) s j)))

(defn shard-of
  "The shard PgDog should route key k to."
  [test k]
  (case (:sharding test)
    (:list :range) (quot k base)
    :hash          (hash-shard k (shard-count test))))

(defn anchor
  "A key on shard s for routing probe statements: the probe table holds one
  row per shard, at that shard's anchor."
  [test s]
  (key-of test s 0))

(defn mapping-toml
  "[[sharded_tables.mapping]] entries for one sharded table, as lines. They
  must follow their [[sharded_tables]] entry."
  [test]
  (let [n (shard-count test)]
    (case (:sharding test)
      :hash  []
      :range (mapcat (fn [s]
                       ["[[sharded_tables.mapping]]"
                        (str "shard = " s)
                        (str "start = " (* s base))
                        (str "end = " (* (inc s) base))])
                     (range n))
      :list  (mapcat (fn [s]
                       ["[[sharded_tables.mapping]]"
                        (str "shard = " s)
                        (str "values = ["
                             (str/join ", " (map #(+ (* s base) %) (range (:list-size test))))
                             "]")])
                     (range n)))))

(defn global-key
  "--cross-shard: key j of one Elle key space spread over every shard,
  round-robin: j lives on shard (mod j n), as that shard's (quot j n)-th key.
  A txn over several keys then usually spans several shards."
  [test j]
  (let [n (shard-count test)]
    (key-of test (mod j n) (quot j n))))
