;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

(ns app.main.data.persistence
  (:require
   [app.common.data :as d]
   [app.common.data.macros :as dm]
   [app.common.logging :as log]
   [app.common.files.changes :as cpc]
   [app.common.types.shape-tree :as ctst]
   [app.common.uuid :as uuid]
   [app.main.data.changes :as dch]
   [app.main.features :as features]
   [app.main.repo :as rp]
   [app.main.store :as st]
   [app.util.router :as rt]
   [app.util.time :as dt]
   [beicon.core :as rx]
   [okulary.core :as l]
   [potok.core :as ptk]))

(declare ^:private run-persistence-task)

(log/set-level! :trace)

(def running (atom false))
(def revn-data (atom {}))
(def queue-conj (fnil conj #queue []))

(defn- update-status
  [status]
  (ptk/reify ::update-status
    ptk/UpdateEvent
    (update [_ state]
      (update state :persistence (fn [pstate]
                                   ;; (log/trc :hint "update-status" :status status :prev-status (:status pstate))
                                   (let [status (if (and (= status :pending)
                                                         (= (:status pstate) :saving))
                                                  (:status pstate)
                                                  status)]

                                     (-> (assoc pstate :status status)
                                         (cond-> (= status :saved)
                                           (dissoc :run-id)))))))))

(defn- update-file-revn
  [file-id revn]
  (ptk/reify ::update-file-revn
    ptk/UpdateEvent
    (update [_ state]
      (log/debug :hint "update-file-revn" :file-id (dm/str file-id) :revn revn)
      (if-let [current-file-id (:current-file-id state)]
        (if (= file-id current-file-id)
          (update-in state [:workspace-file :revn] max revn)
          (d/update-in-when state [:workspace-libraries file-id :revn] max revn))
        state))

    ptk/EffectEvent
    (effect [_ _ _]
      (swap! revn-data update file-id (fnil max 0) revn))))

(defn- discard-commit
  [commit-id]
  (ptk/reify ::discard-commit
    ptk/UpdateEvent
    (update [_ state]
      (update state :persistence (fn [pstate]
                                   (-> pstate
                                       (update :queue (fn [queue]
                                                        (if (= commit-id (peek queue))
                                                          (pop queue)
                                                          (throw (ex-info "invalid state" {})))))
                                       (update :index dissoc commit-id)))))))


(defn- append-commit
  "Event used internally to append the current change to the
  persistence queue."
  [{:keys [id] :as commit}]
  (let [run-id (uuid/next)]
    (ptk/reify ::append-commit
      ptk/UpdateEvent
      (update [_ state]
        (log/trc :hint "append-commit" :method "update" :commit-id (dm/str id))
        (update state :persistence
                (fn [pstate]
                   (-> pstate
                       (update :run-id #(d/nilv % run-id))
                       (update :queue queue-conj id)
                       (update :index assoc id commit)))))

      ptk/WatchEvent
      (watch [_ state stream]
        (let [pstate (:persistence state)]
          (when (= run-id (:run-id pstate))
            (rx/of (run-persistence-task)
                   (update-status :saving))))))))

(defn- persist-commit
  [commit-id]
  (ptk/reify ::persist-commit
    ptk/WatchEvent
    (watch [_ state stream]
      (log/dbg :hint "persist-commit" :commit-id (dm/str commit-id))
      (when-let [{:keys [file-id file-revn changes] :as commit} (dm/get-in state [:persistence :index commit-id])]
        (let [;; this features set does not includes the ffeat/enabled
              ;; because they are already available on the backend and
              ;; this request provides a set of features to enable in
              ;; this request.
              features (features/get-team-enabled-features state)
              sid      (:session-id state)
              revn     (max file-revn (get @revn-data file-id 0))
              params   {:id file-id
                        :revn revn
                        :session-id sid
                        :origin (:origin commit)
                        :created-at (:created-at commit)
                        :commit-id commit-id
                        :changes (vec changes)
                        :features features}]

          ;; FIXME: handle lagged here !!!!
          (->> (rp/cmd! :update-file params)
               (rx/mapcat (fn [{:keys [revn lagged] :as response}]
                            (log/debug :hint "changes persisted" :commit-id (dm/str commit-id) :lagged (count lagged))
                            (rx/of (ptk/data-event ::commit-persisted commit)
                                   (update-file-revn file-id revn))))

               (rx/catch (fn [cause]
                           (rx/concat
                            (if (= :authentication (:type cause))
                              (rx/empty)
                              (rx/of (rt/assign-exception cause)))
                            (rx/throw cause))))))))))


(defn- run-persistence-task
  []
  (ptk/reify ::run-persistence-task
    ptk/WatchEvent
    (watch [_ state stream]
      (let [{:keys [queue index]} (:persistence state)]
        (if-let [commit-id (peek queue)]
          (do
            (log/dbg :hint "run-persistence-task" :commit-id (dm/str commit-id))
            (->> (rx/merge
                  (rx/of (persist-commit commit-id))
                  (->> stream
                       (rx/filter (ptk/type? ::commit-persisted))
                       (rx/map deref)
                       (rx/filter #(= commit-id (:id %)))
                       (rx/take 1)
                       ;; (rx/observe-on :async)
                       (rx/mapcat (fn [_]
                                    (rx/of (discard-commit commit-id)
                                           (run-persistence-task))))))
                 (rx/take-until
                  (rx/filter (ptk/type? ::run-persistence-task) stream))))

          (rx/of (update-status :saved)))))))

(defn- merge-commit
  [buffer]
  (->> (rx/from (group-by :file-id buffer))
       (rx/map (fn [[file-id [item :as commits]]]
                 (let [uchg (into [] (mapcat :undo-changes) buffer)
                       rchg (into [] (mapcat :changes) buffer)]
                   {:id (:id item)
                    :undo-changes uchg
                    :changes rchg
                    :file-id file-id
                    :origin (:origin item)
                    :created-at (:created-at item)})))))

(defn initialize-persistence
  []
  (ptk/reify ::initialize-persistence
    ptk/WatchEvent
    (watch [_ _ stream]
      (log/debug :hint "initialize persistence")
      (let [stoper-s (rx/filter (ptk/type? ::initialize-persistence) stream)

            commits-s
            (->> stream
                 (rx/filter dch/commit?)
                 (rx/map deref)
                 (rx/filter #(= :local (:source %)))
                 (rx/filter (complement empty?))
                 (rx/share))

            notifier-s
            (->> commits-s
                 (rx/debounce 3000)
                 (rx/tap #(log/trc :hint "persistence beat")))]


        (rx/merge
         (->> commits-s
              (rx/debounce 200)
              (rx/map (fn [_]
                        (update-status :pending)))
              (rx/take-until stoper-s))

         ;; Here we watch for local commits, buffer them in a small
         ;; chunks (very near in time commits) and append them to the
         ;; persistence queue
         (->> commits-s
              (rx/buffer-until notifier-s)
              (rx/mapcat merge-commit)
              (rx/mapcat (fn [commit]
                           (rx/of (append-commit commit)
                                  (dch/update-indexes commit))))
              (rx/take-until (rx/delay 100 stoper-s))
              (rx/finalize (fn []
                             (log/debug :hint "finalize persistence: changes watcher"))))

         ;; Here we track all incoming remote commits for maintain
         ;; updated the local state with the file revn
         (->> stream
              (rx/filter dch/commit?)
              (rx/map deref)
              (rx/filter #(= :remote (:source %)))
              (rx/mapcat (fn [{:keys [file-id file-revn] :as commit}]
                           (rx/of (update-file-revn file-id file-revn)
                                  (dch/update-indexes commit))))
              (rx/take-until stoper-s)))))))
