;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.main.data.workspace.path.undo
  (:require
   [app.common.data :as d]
   [app.common.data.undo-stack :as u]
   [app.common.uuid :as uuid]
   [app.main.data.workspace.path.state :as st]
   [app.main.store :as store]
   [beicon.core :as rx]
   [okulary.core :as l]
   [potok.core :as ptk]))

(defn undo-event?
  [event]
  (= :app.main.data.workspace.common/undo (ptk/type event)))

(defn redo-event?
  [event]
  (= :app.main.data.workspace.common/redo (ptk/type event)))

(defn- make-entry [state]
  (let [id (st/get-path-id state)]
    {:content (get-in state (st/get-path state :content))
     :preview (get-in state [:workspace-local :edit-path id :preview])
     :last-point (get-in state [:workspace-local :edit-path id :last-point])
     :prev-handler (get-in state [:workspace-local :edit-path id :prev-handler])}))

(defn- load-entry [state {:keys [content preview last-point prev-handler]}]
  (let [id (st/get-path-id state)]
    (-> state
        (d/assoc-in-when (st/get-path state :content) content)
        (d/update-in-when
         [:workspace-local :edit-path id]
         assoc
         :preview preview
         :last-point last-point
         :prev-handler prev-handler))))

(defn undo []
  (ptk/reify ::undo
    ptk/UpdateEvent
    (update [_ state]
      (let [id (st/get-path-id state)
            undo-stack (-> (get-in state [:workspace-local :edit-path id :undo-stack])
                           (u/undo))
            entry (u/peek undo-stack)]
        (cond-> state
          (some? entry)
          (-> (load-entry entry)
              (d/assoc-in-when
               [:workspace-local :edit-path id :undo-stack]
               undo-stack)))))))

(defn redo []
  (ptk/reify ::redo
    ptk/UpdateEvent
    (update [_ state]
      (let [id (st/get-path-id state)
            undo-stack (-> (get-in state [:workspace-local :edit-path id :undo-stack])
                           (u/redo))
            entry (u/peek undo-stack)]
        (-> state
            (load-entry entry)
            (d/assoc-in-when
             [:workspace-local :edit-path id :undo-stack]
             undo-stack))))))

(defn add-undo-entry []
  (ptk/reify ::add-undo-entry
    ptk/UpdateEvent
    (update [_ state]
      (let [id (st/get-path-id state)
            entry (make-entry state)]
        (-> state
            (d/update-in-when
             [:workspace-local :edit-path id :undo-stack]
             u/append entry))))))

(defn end-path-undo
  []
  (ptk/reify ::end-path-undo
    ptk/UpdateEvent
    (update [_ state]
      (-> state
          (d/update-in-when
           [:workspace-local :edit-path (st/get-path-id state)]
           dissoc :undo-lock :undo-stack)))))

(defn- stop-undo? [event]
  (= :app.main.data.workspace.common/clear-edition-mode (ptk/type event)))

(def path-content-ref
  (letfn [(selector [state]
            (get-in state (st/get-path state :content)))]
    (l/derived selector store/state)))

(defn start-path-undo
  []
  (let [lock (uuid/next)]
    (ptk/reify ::start-path-undo
      ptk/UpdateEvent
      (update [_ state]
        (let [undo-lock (get-in state [:workspace-local :edit-path (st/get-path-id state) :undo-lock])]
          (cond-> state
            (not undo-lock)
            (update-in [:workspace-local :edit-path (st/get-path-id state)]
                       assoc
                       :undo-lock lock
                       :undo-stack (u/make-stack)))))
      
      ptk/WatchEvent
      (watch [_ state stream]
        (let [undo-lock (get-in state [:workspace-local :edit-path (st/get-path-id state) :undo-lock])]
          (when (= undo-lock lock)
            (let [stop-undo-stream (->> stream
                                        (rx/filter stop-undo?)
                                        (rx/take 1))]
              (rx/concat
               (->> (rx/merge
                     (->> (rx/from-atom path-content-ref {:emit-current-value? true})
                          (rx/filter (comp not nil?))
                          (rx/map #(add-undo-entry)))

                     (->> stream
                          (rx/filter undo-event?)
                          (rx/map #(undo)))

                     (->> stream
                          (rx/filter redo-event?)
                          (rx/map #(redo))))

                    (rx/take-until stop-undo-stream))

               (rx/of (end-path-undo))))))))))

