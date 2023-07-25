;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

(ns app.main.data.workspace.changes
  (:require
   [app.common.data :as d]
   [app.common.data.macros :as dm]
   [app.common.exceptions :as ex]
   [app.common.files.changes :as cpc]
   [app.common.files.changes-builder :as pcb]
   [app.common.files.helpers :as cph]
   [app.common.logging :as log]
   [app.common.schema :as sm]
   [app.common.types.shape-tree :as ctst]
   [app.common.uuid :as uuid]
   [app.main.data.workspace.state-helpers :as wsh]
   [app.main.data.workspace.undo :as dwu]
   [app.main.store :as st]
   [app.main.worker :as uw]
   [app.util.time :as dt]
   [beicon.core :as rx]
   [potok.core :as ptk]))

;; Change this to :info :debug or :trace to debug this module
(log/set-level! :debug)

(defonce page-change? #{:add-page :mod-page :del-page :mov-page})
(defonce update-layout-attr? #{:hidden})

(declare commit-changes)

(defn- add-undo-group
  [changes state]
  (let [undo            (:workspace-undo state)
        items           (:items undo)
        index           (or (:index undo) (dec (count items)))
        prev-item       (when-not (or (empty? items) (= index -1))
                          (get items index))
        undo-group      (:undo-group prev-item)
        add-undo-group? (and
                         (not (nil? undo-group))
                         (= (get-in changes [:redo-changes 0 :type]) :mod-obj)
                         (= (get-in prev-item [:redo-changes 0 :type]) :add-obj)
                         (contains? (:tags prev-item) :alt-duplication))] ;; This is a copy-and-move with mouse+alt

    (cond-> changes add-undo-group? (assoc :undo-group undo-group))))

(def commit? (ptk/type? ::commit))

(defn update-shapes
  ([ids update-fn] (update-shapes ids update-fn nil))
  ([ids update-fn {:keys [reg-objects? save-undo? stack-undo? attrs ignore-tree page-id ignore-remote? ignore-touched undo-group]
                   :or {reg-objects? false save-undo? true stack-undo? false ignore-remote? false ignore-touched false}}]
   (dm/assert!
    "expected a valid coll of uuid's"
    (sm/check-coll-of-uuid! ids))

   (dm/assert!
    "expected `update-fn` to be a fn"
    (fn? update-fn))

   (ptk/reify ::update-shapes
     ptk/WatchEvent
     (watch [it state _]
       (let [page-id   (or page-id (:current-page-id state))
             objects   (wsh/lookup-page-objects state page-id)
             ids       (into [] (filter some?) ids)

             update-layout-ids
             (->> ids
                  (map (d/getf objects))
                  (filter #(some update-layout-attr? (pcb/changed-attrs % update-fn {:attrs attrs})))
                  (map :id))

             changes   (reduce
                        (fn [changes id]
                          (let [opts {:attrs attrs
                                      :ignore-geometry? (get ignore-tree id)
                                      :ignore-touched ignore-touched}]
                            (pcb/update-shapes changes [id] update-fn (d/without-nils opts))))
                        (-> (pcb/empty-changes it page-id)
                            (pcb/set-save-undo? save-undo?)
                            (pcb/set-stack-undo? stack-undo?)
                            (pcb/with-objects objects)
                            (cond-> undo-group
                              (pcb/set-undo-group undo-group)))
                        ids)
             changes (pcb/reorder-grid-children changes ids)
             changes (add-undo-group changes state)]
         (rx/concat
          (if (seq (:redo-changes changes))
            (let [changes  (cond-> changes reg-objects? (pcb/resize-parents ids))
                  changes (cond-> changes ignore-remote? (pcb/ignore-remote))]
              (rx/of (commit-changes changes)))
            (rx/empty))

          ;; Update layouts for properties marked
          (if (d/not-empty? update-layout-ids)
            (rx/of (ptk/data-event :layout/update update-layout-ids))
            (rx/empty))))))))

;; (defn send-update-indices
;;   []
;;   (ptk/reify ::send-update-indices
;;     ptk/WatchEvent
;;     (watch [_ _ _]
;;       (->> (rx/of
;;             (fn [state]
;;               (-> state
;;                   (dissoc ::update-indices-debounce)
;;                   (dissoc ::update-changes))))
;;            (rx/observe-on :async)))

;;     ptk/EffectEvent
;;     (effect [_ state _]
;;       (doseq [[page-id changes] (::update-changes state)]
;;         (uw/ask! {:cmd :update-page-index
;;                   :page-id page-id
;;                   :changes changes})))))

;; ;; Update indices will debounce operations so we don't have to update
;; ;; the index several times (which is an expensive operation)
;; (defn update-indices
;;   [page-id changes]

;;   (let [start (uuid/next)]
;;     (ptk/reify ::update-indices
;;       ptk/UpdateEvent
;;       (update [_ state]
;;         (if (nil? (::update-indices-debounce state))
;;           (assoc state ::update-indices-debounce start)
;;           (update-in state [::update-changes page-id] (fnil d/concat-vec []) changes)))

;;       ptk/WatchEvent
;;       (watch [_ state stream]
;;         (if (= (::update-indices-debounce state) start)
;;           (let [stopper (->> stream (rx/filter (ptk/type? :app.main.data.workspace/finalize)))]
;;             (rx/merge
;;              (->> stream
;;                   (rx/filter (ptk/type? ::update-indices))
;;                   (rx/debounce 50)
;;                   (rx/take 1)
;;                   (rx/map #(send-update-indices))
;;                   (rx/take-until stopper))
;;              (rx/of (update-indices page-id changes))))
;;           (rx/empty))))))

(defn update-indexes
  [{:keys [changes] :as commit}]
  (ptk/reify ::update-indexes
    ptk/WatchEvent
    (watch [_ _ _]
      (let [changes (->> changes
                         (map (fn [{:keys [id type page] :as change}]
                                (cond-> change
                                  (and (page-change? type) (nil? (:page-id change)))
                                  (assoc :page-id (or id (:id page))))))
                         (filter :page-id)
                         (group-by :page-id))]

        (->> (rx/from changes)
             (rx/merge-map (fn [[page-id changes]]
                             (log/debug :hint "update-indexes" :page-id page-id :changes (count changes))
                             (uw/ask! {:cmd :update-page-index
                                       :page-id page-id
                                       :changes changes})))
             (rx/ignore))))))

;; (defn- changed-frames
;;   "Extracts the frame-ids changed in the given changes"
;;   [changes objects]

;;   (let [change->ids
;;         (fn [change]
;;           (case (:type change)
;;             :add-obj
;;             [(:parent-id change)]

;;             (:mod-obj :del-obj)
;;             [(:id change)]

;;             :mov-objects
;;             (d/concat-vec (:shapes change) [(:parent-id change)])

;;             []))]
;;     (into #{}
;;           (comp (mapcat change->ids)
;;                 (keep #(cph/get-shape-id-root-frame objects %))
;;                 (remove #(= uuid/zero %)))
;;           changes)))

(defn commit
  [{:keys [commit-id redo-changes undo-changes origin save-undo? #_affected-frames
           file-id file-revn page-id undo-group tags stack-undo? source]}]

  (dm/assert!
   "expect valid vector of changes"
   (and (cpc/valid-changes? redo-changes)
        (cpc/valid-changes? undo-changes)))

  (let [commit-id (or commit-id (uuid/next))
        commit    {:id commit-id
                   :created-at (dt/now)
                   :source (d/nilv :local source)
                   :origin (ptk/type origin)
                   :file-id file-id
                   :file-revn file-revn
                   :changes redo-changes
                   :redo-changes redo-changes
                   :undo-changes undo-changes
                   ;; :frames affected-frames
                   :save-undo? save-undo?
                   :undo-group undo-group
                   :tags tags
                   :stack-undo? stack-undo?}]

    (ptk/reify ::commit
      cljs.core/IDeref
      (-deref [_] commit)

      ptk/UpdateEvent
      (update [_ state]
        (let [current-file-id (get state :current-file-id)
              path            (if (= file-id current-file-id)
                                [:workspace-data]
                                [:workspace-libraries file-id :data])]

          (d/update-in-when state path (fn [file]
                                         (let [file (cp/process-changes file redo-changes false)
                                               pids (into #{} (map :page-id) redo-changes)]
                                           (reduce #(ctst/update-object-indices %1 %2) file pids)))))))))


(defn- resolve-file-revn
  [state file-id]
  (let [file (:workspace-file state)]
    (if (= (:id file) file-id)
      (:revn file)
      (dm/get-in state [:workspace-libraries file-id :revn]))))

(defn commit-changes
  "Schedules a list of changes to execute now, and add the corresponding undo changes to
   the undo stack.

   Options:
   - save-undo?: if set to false, do not add undo changes.
   - undo-group: if some consecutive changes (or even transactions) share the same
                 undo-group, they will be undone or redone in a single step
   "
  [{:keys [redo-changes undo-changes origin save-undo? undo-group tags stack-undo? file-id]
    :or {save-undo? true
         stack-undo? false
         undo-group (uuid/next)
         tags #{}}
    :as params}]
  (ptk/reify ::commit-changes
    ptk/WatchEvent
    (watch [_ state _]
      (let [;; FIXME: :fire: looks unused right now
            ;; frames  (changed-frames redo-changes (wsh/lookup-page-objects state))
            file-id (or file-id (:current-file-id state))
            uchg    (vec undo-changes)
            rchg    (vec redo-changes)]

        (rx/merge
         (rx/of (-> params
                    (assoc :undo-group undo-group)
                    (assoc :tags tags)
                    (assoc :stack-undo? stack-undo?)
                    (assoc :save-undo? save-undo?)
                    (assoc :file-id file-id)
                    ;; FIXME: maybe move this to handle 100% on the persistence layer ??
                    (assoc :file-revn (resolve-file-revn state file-id))
                    ;; FIXME: :fire: looks unnused right now
                    ;; (assoc :affected-frames frames)
                    (assoc :undo-changes uchg)
                    (assoc :redo-changes rchg)
                    (commit)))

         ;; PROCESS UNDO
         (when (and save-undo? (seq undo-changes))
           (let [entry {:undo-changes uchg
                        :redo-changes rchg
                        :undo-group undo-group
                        :tags tags}]
             (rx/of (dwu/append-undo entry stack-undo?)))))))))
